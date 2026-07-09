#include <lineairdb/pax_store.h>

#include <algorithm>
#include <cassert>
#include <charconv>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

namespace LineairDB {
namespace Pax {

namespace {

// ---------------------------------------------------------------------------
// Typed cell codec. Parses the proxy's ASCII val_str into a fixed-width LE
// binary at scatter time, and reformats the binary back into the *exact*
// original val_str ASCII at gather time. The round trip must be byte-identical:
// DataBuffer::size tracks the original ASCII payload size, so a gather that
// renders a different length would corrupt the row. Any parse/range failure
// returns false so the caller takes the heap fallback (never wrong, just
// unaccelerated) -- exactly like an over-wide UNTYPED cell.
// ---------------------------------------------------------------------------

// int64 from a full ASCII integer (from_chars, whole span consumed).
inline bool ParseI64(const char* s, size_t len, int64_t* out) {
  if (len == 0) return false;
  const auto res = std::from_chars(s, s + len, *out);
  return res.ec == std::errc() && res.ptr == s + len;
}

// "YYYY-MM-DD" -> YYYYMMDD (fits int32; string order == int order).
inline bool ParseDate(const char* s, size_t len, int64_t* out) {
  if (len != 10 || s[4] != '-' || s[7] != '-') return false;
  int64_t y = 0, m = 0, d = 0;
  auto digs = [](const char* p, int n, int64_t* o) {
    for (int i = 0; i < n; i++) {
      if (p[i] < '0' || p[i] > '9') return false;
      *o = *o * 10 + (p[i] - '0');
    }
    return true;
  };
  if (!digs(s, 4, &y) || !digs(s + 5, 2, &m) || !digs(s + 8, 2, &d))
    return false;
  *out = y * 10000 + m * 100 + d;
  return true;
}

// Exact DECIMAL(p,s) val_str -> scaled int64 (value * 10^scale). The input has
// the column's declared scale of fractional digits (MySQL pads), so scaling is
// exact -- no double, so no rounding trap here.
inline bool ParseDecScaled(const char* s, size_t len, int scale, int64_t* out) {
  if (len == 0) return false;
  size_t i = 0;
  bool neg = false;
  if (s[0] == '-') {
    neg = true;
    i = 1;
  } else if (s[0] == '+') {
    i = 1;
  }
  __int128 m = 0;
  int fdig = 0;
  bool seen_dot = false, any = false;
  for (; i < len; i++) {
    const char c = s[i];
    if (c == '.') {
      if (seen_dot) return false;
      seen_dot = true;
      continue;
    }
    if (c < '0' || c > '9') return false;
    m = m * 10 + (c - '0');
    if (seen_dot) fdig++;
    any = true;
  }
  if (!any) return false;
  // Normalize to the declared scale (val_str emits exactly `scale` fractionals,
  // but tolerate fewer by padding -- never silently drop precision).
  if (fdig > scale) return false;
  while (fdig < scale) {
    m *= 10;
    fdig++;
  }
  if (neg) m = -m;
  if (m > INT64_MAX || m < INT64_MIN) return false;
  *out = static_cast<int64_t>(m);
  return true;
}

// Parse one field's ASCII into the low bytes of *out. Returns false on any
// failure (caller -> heap fallback).
inline bool ParseTyped(uint8_t kind, int scale, const std::byte* payload,
                       uint32_t len, uint64_t* out) {
  const char* s = reinterpret_cast<const char*>(payload);
  switch (kind) {
    case FK_INT32: {
      int64_t v;
      if (!ParseI64(s, len, &v) || v < INT32_MIN || v > INT32_MAX) return false;
      *out = static_cast<uint64_t>(v);
      return true;
    }
    case FK_INT64: {
      int64_t v;
      if (!ParseI64(s, len, &v)) return false;
      *out = static_cast<uint64_t>(v);
      return true;
    }
    case FK_DATE: {
      int64_t ymd;
      if (!ParseDate(s, len, &ymd)) return false;
      *out = static_cast<uint64_t>(ymd);
      return true;
    }
    case FK_DEC64: {
      int64_t m;
      if (!ParseDecScaled(s, len, scale, &m)) return false;
      *out = static_cast<uint64_t>(m);
      return true;
    }
    default:
      return false;
  }
}

inline void AppendI64(std::string& out, int64_t v) {
  char buf[24];
  const auto res = std::to_chars(buf, buf + sizeof(buf), v);
  out.append(buf, res.ptr);
}

// Format a typed cell's `width` LE bytes back into the exact val_str ASCII.
// `cell` points at the payload (>= width bytes readable -- the cell stride
// reserves them, so this is memory-safe even under a torn read).
void FormatTyped(uint8_t kind, int scale, const std::byte* cell, uint32_t width,
                 std::string& out) {
  (void)width;
  switch (kind) {
    case FK_INT32: {
      int32_t v;
      std::memcpy(&v, cell, 4);
      AppendI64(out, v);
      break;
    }
    case FK_INT64: {
      int64_t v;
      std::memcpy(&v, cell, 8);
      AppendI64(out, v);
      break;
    }
    case FK_DATE: {
      int32_t v;
      std::memcpy(&v, cell, 4);
      const int64_t y = v / 10000, m = (v / 100) % 100, d = v % 100;
      char buf[16];
      // %04d-%02d-%02d, matching MySQL DATE val_str.
      const int n = std::snprintf(buf, sizeof(buf), "%04lld-%02lld-%02lld",
                                  static_cast<long long>(y),
                                  static_cast<long long>(m),
                                  static_cast<long long>(d));
      if (n > 0) out.append(buf, static_cast<size_t>(n));
      break;
    }
    case FK_DEC64: {
      int64_t m;
      std::memcpy(&m, cell, 8);
      const bool neg = m < 0;
      __int128 mm = neg ? -static_cast<__int128>(m) : m;
      std::string digits;
      if (mm == 0) {
        digits = "0";
      } else {
        while (mm) {
          digits.push_back(static_cast<char>('0' + int(mm % 10)));
          mm /= 10;
        }
        std::reverse(digits.begin(), digits.end());
      }
      while (static_cast<int>(digits.size()) <= scale)
        digits.insert(digits.begin(), '0');
      if (neg) out.push_back('-');
      if (scale == 0) {
        out += digits;
      } else {
        const size_t ip = digits.size() - static_cast<size_t>(scale);
        out.append(digits, 0, ip);
        out.push_back('.');
        out.append(digits, ip, std::string::npos);
      }
      break;
    }
    default:
      break;
  }
}

// Proxy row marker for an empty/no-value field.
constexpr std::byte kNoValue{0xFF};

/**
 * @brief Returns the minimum little-endian base-256 byte count for `len`.
 *
 * @details This mirrors
 * `LineairDBField::calculate_minimum_byte_size_required()` in the proxy row
 * codec so that gathered rows are byte-identical to proxy rows.
 */
inline uint32_t LengthPrefixBytes(uint32_t len) {
  uint32_t n = 0;
  for (uint32_t v = len; v > 0; v /= 256) n++;
  return n;
}

// Reference to one decoded field payload inside a proxy row.
struct FieldRef {
  const std::byte* payload;
  uint32_t len;
};

/**
 * @brief Parses proxy row bytes into per-field payload references.
 *
 * @param row Proxy row payload bytes.
 * @param size Number of bytes in `row`.
 * @param out Destination array for decoded field references.
 * @param max_fields Maximum number of entries available in `out`.
 * @return Number of decoded fields, or `SIZE_MAX` when the input is malformed
 * or contains more than `max_fields` fields.
 */
size_t ParseRow(const std::byte* row, size_t size, FieldRef* out,
                size_t max_fields) {
  size_t off = 0;
  size_t n = 0;
  while (off < size) {
    if (n == max_fields) return SIZE_MAX;
    const auto byte_size = static_cast<uint8_t>(row[off]);
    off += 1;
    uint32_t len = 0;
    if (byte_size != 0xFF) {
      if (byte_size > 4 || off + byte_size > size) return SIZE_MAX;
      for (uint32_t i = 0; i < byte_size; i++) {
        len |= static_cast<uint32_t>(static_cast<uint8_t>(row[off + i]))
               << (8 * i);
      }
      off += byte_size;
      if (off + len > size) return SIZE_MAX;
    }
    out[n].payload = row + off;
    out[n].len = len;
    off += len;
    n++;
  }
  return (off == size) ? n : SIZE_MAX;
}
}  // namespace

PaxGroup::PaxGroup(const TableSchema& schema, PaxStore* store)
    : schema_(schema), store_(store) {
  const size_t fields = schema.field_count();
  stride_.resize(fields);
  strip_offset_.resize(fields);
  size_t total = 0;
  for (size_t f = 0; f < fields; f++) {
    stride_[f] = kCellLenBytes + schema.field_max_bytes[f];
    total = (total + 63) & ~size_t{63};  // 64B-align each strip
    strip_offset_[f] = total;
    total += static_cast<size_t>(stride_[f]) * kRows;
  }
  arena_.reset(new std::byte[total]());  // zero-init: len=0 everywhere
  // Allocate visibility flags for this group's slots.
  visible_.reset(new std::atomic<uint64_t>[kRows / 64]());
}

bool PaxGroup::ScatterRow(uint32_t slot, const std::byte* row, size_t size) {
  assert(slot < kRows);
  const size_t fields = schema_.field_count();
  // Stack refs keep typical rows allocation-free; unusually wide tables take
  // the heap fallback instead.
  constexpr size_t kMaxFields = 512;
  if (fields > kMaxFields) return false;
  FieldRef refs[kMaxFields];
  const size_t parsed = ParseRow(row, size, refs, fields);
  if (parsed != fields) return false;
  // Validate every field before any cell write. UNTYPED fields must fit their
  // cell width; typed non-null fields are parsed into a fixed-width LE binary
  // scratch. Any failure takes the per-row heap fallback with the slot
  // untouched -- the write_counter is only bumped once every field validates.
  const bool has_kinds = !schema_.field_kind.empty();
  uint64_t typed_bin[kMaxFields];  // low field_max_bytes[f] bytes = LE payload
  for (size_t f = 0; f < fields; f++) {
    const uint8_t k = has_kinds ? schema_.field_kind[f] : FK_UNTYPED;
    if (k == FK_UNTYPED) {
      if (refs[f].len > schema_.field_max_bytes[f]) return false;
      if (refs[f].len > 0xFFFF) return false;
    } else if (refs[f].len != 0) {  // typed present value
      if (!ParseTyped(k, schema_.scale_of(f), refs[f].payload, refs[f].len,
                      &typed_bin[f]))
        return false;
    }
  }
  write_counter.fetch_add(1, std::memory_order_release);
  for (size_t f = 0; f < fields; f++) {
    std::byte* cell = arena_.get() + strip_offset_[f] +
                      static_cast<size_t>(stride_[f]) * slot;
    const uint8_t k = has_kinds ? schema_.field_kind[f] : FK_UNTYPED;
    if (k != FK_UNTYPED && refs[f].len != 0) {
      const uint16_t len = static_cast<uint16_t>(schema_.field_max_bytes[f]);
      std::memcpy(cell, &len, sizeof(len));
      std::memcpy(cell + kCellLenBytes, &typed_bin[f], len);  // low `len` = LE
      continue;
    }
    // UNTYPED, or a typed NULL (len 0): store verbatim (0 => empty == NULL).
    const uint16_t len = static_cast<uint16_t>(refs[f].len);
    std::memcpy(cell, &len, sizeof(len));
    if (len > 0) std::memcpy(cell + kCellLenBytes, refs[f].payload, len);
  }
  // Publish this slot to strip-direct readers after the cells are written.
  visible_[slot >> 6].fetch_or(uint64_t{1} << (slot & 63),
                               std::memory_order_release);
  write_counter.fetch_add(1, std::memory_order_release);
  return true;
}

void PaxGroup::RetireSlot(uint32_t slot) {
  assert(slot < kRows);
  write_counter.fetch_add(1, std::memory_order_release);
  // Hide this slot from strip-direct readers.
  visible_[slot >> 6].fetch_and(~(uint64_t{1} << (slot & 63)),
                                std::memory_order_release);
  write_counter.fetch_add(1, std::memory_order_release);
}

size_t PaxGroup::GatherRow(uint32_t slot, std::byte* dst,
                           size_t expected_size) const {
  assert(slot < kRows);
  const size_t fields = schema_.field_count();
  const bool has_kinds = !schema_.field_kind.empty();
  std::string scratch;  // reused typed->ASCII buffer (no per-field alloc)
  size_t off = 0;
  for (size_t f = 0; f < fields; f++) {
    const std::byte* cell = arena_.get() + strip_offset_[f] +
                            static_cast<size_t>(stride_[f]) * slot;
    uint16_t len;
    std::memcpy(&len, cell, sizeof(len));
    // Clamp against the cell width: a torn read can produce garbage but must
    // stay memory-safe. The caller's TID re-check rejects torn rows.
    if (len > schema_.field_max_bytes[f]) len = 0;
    if (len == 0) {
      if (off + 1 > expected_size) return off;
      dst[off++] = kNoValue;
      continue;
    }
    const uint8_t k = has_kinds ? schema_.field_kind[f] : FK_UNTYPED;
    const char* src;
    uint32_t vlen;
    if (k != FK_UNTYPED) {
      scratch.clear();
      FormatTyped(k, schema_.scale_of(f), cell + kCellLenBytes,
                  schema_.field_max_bytes[f], scratch);
      src = scratch.data();
      vlen = static_cast<uint32_t>(scratch.size());
    } else {
      src = reinterpret_cast<const char*>(cell + kCellLenBytes);
      vlen = len;
    }
    const uint32_t prefix = LengthPrefixBytes(vlen);
    if (off + 1 + prefix + vlen > expected_size) return off;
    dst[off++] = static_cast<std::byte>(prefix);
    for (uint32_t i = 0; i < prefix; i++) {
      dst[off++] = static_cast<std::byte>((vlen >> (8 * i)) & 0xFF);
    }
    std::memcpy(dst + off, src, vlen);
    off += vlen;
  }
  return off;
}

namespace {

void AppendField(std::string& out, std::string_view payload) {
  if (payload.empty()) {
    out.push_back(static_cast<char>(0xFF));
    return;
  }
  const uint32_t len = static_cast<uint32_t>(payload.size());
  const uint32_t prefix = LengthPrefixBytes(len);
  out.push_back(static_cast<char>(prefix));
  for (uint32_t i = 0; i < prefix; i++) {
    out.push_back(static_cast<char>((len >> (8 * i)) & 0xFF));
  }
  out.append(payload.data(), payload.size());
}

}  // namespace

void PaxGroup::AppendCellField(uint32_t field, uint32_t slot,
                               std::string& out) const {
  const std::string_view cv = cell(field, slot);
  const uint8_t k = schema_.kind_of(field);
  if (k == FK_UNTYPED || cv.empty()) {
    AppendField(out, cv);
    return;
  }
  const std::byte* c = arena_.get() + strip_offset_[field] +
                       static_cast<size_t>(stride_[field]) * slot;
  std::string tmp;
  FormatTyped(k, schema_.scale_of(field), c + kCellLenBytes,
              schema_.field_max_bytes[field], tmp);
  AppendField(out, tmp);
}

bool PaxGroup::GatherRowProjected(uint32_t slot, const uint32_t* columns,
                                  size_t n_columns, std::string& out) const {
  assert(slot < kRows);
  const size_t fields = schema_.field_count();
  AppendCellField(0, slot, out);  // null-flags field (always UNTYPED)
  for (size_t i = 0; i < n_columns; i++) {
    const size_t field = static_cast<size_t>(columns[i]) + 1;
    if (field >= fields) return false;
    AppendCellField(static_cast<uint32_t>(field), slot, out);
  }
  return true;
}

void PaxGroup::GatherRowMasked(uint32_t slot, const uint32_t* columns,
                               size_t n_columns, std::string& out) const {
  const size_t fields = schema_.field_count();
  AppendCellField(0, slot, out);  // null-flags field (always UNTYPED)

  size_t column_index = 0;
  for (size_t field = 1; field < fields; ++field) {
    if (column_index < n_columns &&
        static_cast<size_t>(columns[column_index]) + 1 == field) {
      AppendCellField(static_cast<uint32_t>(field), slot, out);
      ++column_index;
      continue;
    }
    out.push_back(static_cast<char>(0xFF));
  }
}

PaxStore::PaxStore(TableSchema schema) : schema_(std::move(schema)) {
  dir_.reset(new std::atomic<PaxGroup*>[kMaxGroups]());
}

std::pair<PaxGroup*, uint32_t> PaxStore::AllocateSlot() {
  const uint64_t idx = next_slot_.fetch_add(1, std::memory_order_relaxed);
  const uint64_t group_idx = idx / PaxGroup::kRows;
  if (group_idx >= kMaxGroups) return {nullptr, 0};
  PaxGroup* grp = dir_[group_idx].load(std::memory_order_acquire);
  if (grp == nullptr) {
    std::lock_guard<std::mutex> lk(grow_mutex_);
    grp = dir_[group_idx].load(std::memory_order_acquire);
    if (grp == nullptr) {
      grp = new PaxGroup(schema_, this);
      dir_[group_idx].store(grp, std::memory_order_release);
    }
  }
  return {grp, static_cast<uint32_t>(idx % PaxGroup::kRows)};
}

}  // namespace Pax
}  // namespace LineairDB
