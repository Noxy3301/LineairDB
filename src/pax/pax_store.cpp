#include "pax/pax_store.h"

#include <cassert>
#include <cstring>

namespace LineairDB {
namespace Pax {

namespace {

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

PaxGroup::PaxGroup(const TableSchema& schema) : schema_(schema) {
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
  for (size_t f = 0; f < fields; f++) {
    if (refs[f].len > schema_.field_max_bytes[f]) return false;
    if (refs[f].len > 0xFFFF) return false;
  }
  write_counter.fetch_add(1, std::memory_order_release);
  for (size_t f = 0; f < fields; f++) {
    std::byte* cell = arena_.get() + strip_offset_[f] +
                      static_cast<size_t>(stride_[f]) * slot;
    const uint16_t len = static_cast<uint16_t>(refs[f].len);
    std::memcpy(cell, &len, sizeof(len));
    if (len > 0) std::memcpy(cell + kCellLenBytes, refs[f].payload, len);
  }
  write_counter.fetch_add(1, std::memory_order_release);
  return true;
}

size_t PaxGroup::GatherRow(uint32_t slot, std::byte* dst,
                           size_t expected_size) const {
  assert(slot < kRows);
  const size_t fields = schema_.field_count();
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
    const uint32_t prefix = LengthPrefixBytes(len);
    if (off + 1 + prefix + len > expected_size) return off;
    dst[off++] = static_cast<std::byte>(prefix);
    for (uint32_t i = 0; i < prefix; i++) {
      dst[off++] = static_cast<std::byte>((len >> (8 * i)) & 0xFF);
    }
    std::memcpy(dst + off, cell + kCellLenBytes, len);
    off += len;
  }
  return off;
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
      grp = new PaxGroup(schema_);
      dir_[group_idx].store(grp, std::memory_order_release);
    }
  }
  return {grp, static_cast<uint32_t>(idx % PaxGroup::kRows)};
}

}  // namespace Pax
}  // namespace LineairDB
