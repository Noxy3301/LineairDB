#ifndef LINEAIRDB_PAX_STORE_H
#define LINEAIRDB_PAX_STORE_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace LineairDB {
namespace Pax {

/**
 * @brief Per-field storage kind for typed numeric cells.
 *
 * @details FK_UNTYPED keeps the cell verbatim (the original ASCII val_str
 * bytes). The typed kinds shred the numeric value into a fixed-width
 * little-endian binary payload whose width is `field_max_bytes[f]` (4 or 8). A
 * typed cell's u16 length prefix is 0 for SQL NULL and equal to the binary
 * width for a present value, so "empty cell == NULL" still holds. `ScatterRow`
 * parses the ASCII once (heap fallback on any parse/range failure);
 * `GatherRow` reformats the binary back into the exact original val_str ASCII
 * (byte-identical round trip -- the row-format contract).
 */
enum FieldKind : uint8_t {
  FK_UNTYPED = 0,  // verbatim bytes (default; strings, floats, DECIMAL pre-DEC64)
  FK_INT32 = 1,    // 4-byte LE signed int   (TINY/SHORT/INT24/LONG)
  FK_INT64 = 2,    // 8-byte LE signed int   (LONG UNSIGNED, BIGINT)
  FK_DATE = 3,     // 4-byte LE YYYYMMDD int (DATE)
  FK_DEC64 = 4,    // 8-byte LE scaled int   (DECIMAL(p,s); scale=field_scale)
};

/**
 * @brief Describes the proxy row fields that a PAX-enabled table stores in
 * strips.
 *
 * @details The proxy row payload is laid out as one null-flags field followed
 * by one field per MySQL column. Each field is encoded as
 * `[byte_size:1][value_length:byte_size little-endian][value_bytes]`, with
 * `byte_size == 0xFF` representing an empty/no-value field. `PaxGroup` uses
 * the maximum payload byte widths here to split a row into fixed-width cells.
 * UNTYPED cells store `[u16 len][payload up to field_max_bytes]`; typed cells
 * store `[u16 len][fixed-width LE binary]` with `field_max_bytes` equal to the
 * binary width (4/8).
 */
struct TableSchema {
  // Max payload bytes per field, starting with the null-flags field.
  std::vector<uint32_t> field_max_bytes;
  // Per-field storage kind (see FieldKind). Empty => every field UNTYPED
  // (byte-identical to the untyped layout). Same length as field_max_bytes.
  std::vector<uint8_t> field_kind;
  // Per-field DECIMAL scale for FK_DEC64 (else 0). Same length when present.
  std::vector<int8_t> field_scale;
  // Table name, carried for diagnostics (heap-fallback logging).
  std::string table_name;

  /**
   * @brief Returns the number of encoded fields in a proxy row payload.
   */
  size_t field_count() const { return field_max_bytes.size(); }

  /**
   * @brief Returns the storage kind of field `f` (UNTYPED when untyped).
   */
  uint8_t kind_of(size_t f) const {
    return f < field_kind.size() ? field_kind[f] : FK_UNTYPED;
  }

  /**
   * @brief Returns the DECIMAL scale of field `f` (0 when untyped).
   */
  int scale_of(size_t f) const {
    return f < field_scale.size() ? field_scale[f] : 0;
  }
};

class PaxStore;

/**
 * @brief Stores a fixed-size row group as per-field PAX strips.
 *
 * @details Each logical row occupies the same slot number in every field
 * strip. The row's bytes are stored in the strip cells; `DataItem` continues
 * to own the transaction id word, so Silo validation remains outside this
 * storage class. Callers invoke `ScatterRow()` only while holding the row's
 * existing Silo TID lock. `GatherRow()` is memory-safe under a concurrent
 * scatter to the same slot, and callers reject torn rows with the normal TID
 * re-check. Strip-direct readers can use `IsVisible()` and `write_counter` to
 * read a whole group without materializing rows, then fall back when the group
 * changed during the read.
 */
class PaxGroup {
 public:
  // Number of row slots in one group.
  static constexpr uint32_t kRows = 8192;

  // Number of bytes used for the little-endian uint16_t cell length.
  static constexpr uint32_t kCellLenBytes = 2;

  /**
   * @brief Creates an empty group whose strips are sized from `schema`.
   *
   * @param schema Table schema owned by `PaxStore`; must outlive this group.
   */
  PaxGroup(const TableSchema& schema, PaxStore* store);

  /**
   * @brief Scatters one proxy row payload into this group's strip cells.
   *
   * @param slot Target slot inside this group.
   * @param row Proxy row payload bytes.
   * @param size Number of bytes in `row`.
   * @return false without writing any cell when the payload does not match the
   * schema shape or when any field exceeds its configured cell width.
   */
  bool ScatterRow(uint32_t slot, const std::byte* row, size_t size);

  /**
   * @brief Marks one slot as invisible to strip-direct readers.
   *
   * @param slot Target slot inside this group.
   */
  void RetireSlot(uint32_t slot);

  /**
   * @brief Gathers one slot's strip cells back into a byte-identical proxy row.
   *
   * @param slot Source slot inside this group.
   * @param dst Destination buffer with room for `expected_size` bytes.
   * @param expected_size Row payload length tracked in `DataBuffer::size`.
   * @return Number of bytes written, equal to `expected_size` when the slot is
   * quiet and the stored row is intact.
   */
  size_t GatherRow(uint32_t slot, std::byte* dst, size_t expected_size) const;

  /**
   * @brief Gathers the null-flags field and selected columns into `out`.
   *
   * @param slot Source slot inside this group.
   * @param columns Zero-based MySQL column indexes to gather.
   * @param n_columns Number of entries in `columns`.
   * @param out Destination string; gathered bytes are appended.
   * @return false when a column index is outside this group's schema.
   */
  bool GatherRowProjected(uint32_t slot, const uint32_t* columns,
                          size_t n_columns, std::string& out) const;

  /**
   * @brief Gathers a row-shaped payload with unselected columns masked out.
   *
   * @details The null-flags field is always gathered. Zero-based MySQL columns
   * listed in `columns` are gathered from their strips; unlisted columns are
   * emitted as one-byte empty fields, preserving field indexes without reading
   * unused strips.
   *
   * @param slot Source slot inside this group.
   * @param columns Zero-based MySQL column indexes to materialize, in ascending
   * order.
   * @param n_columns Number of entries in `columns`.
   * @param out Destination string; gathered bytes are appended.
   */
  void GatherRowMasked(uint32_t slot, const uint32_t* columns,
                       size_t n_columns, std::string& out) const;

  /**
   * @brief Returns whether strip-direct readers should consider `slot` live.
   *
   * @param slot Slot inside this group.
   */
  bool IsVisible(uint32_t slot) const {
    // Read this slot's visibility flag for strip-direct scans.
    return (visible_[slot >> 6].load(std::memory_order_acquire) >>
            (slot & 63)) &
           1u;
  }

  /**
   * @brief Returns a memory-safe view of one cell payload.
   *
   * @param field Field index, where 0 is the null-flags field and MySQL column
   * i is field i + 1.
   * @param slot Slot inside this group.
   */
  std::string_view cell(size_t field, uint32_t slot) const {
    const std::byte* cell = arena_.get() + strip_offset_[field] +
                            static_cast<size_t>(stride_[field]) * slot;
    uint16_t len;
    std::memcpy(&len, cell, sizeof(len));
    if (len > schema_.field_max_bytes[field]) len = 0;
    return std::string_view(reinterpret_cast<const char*>(cell) + kCellLenBytes,
                            len);
  }

  /**
   * @brief Appends one field's proxy-format value into `out`.
   *
   * @details Verbatim for an UNTYPED cell; reformatted to the exact val_str
   * ASCII for a typed present cell. An empty cell is emitted as a NULL field.
   * Used by the projected and masked gathers.
   *
   * @param field Field index, where 0 is the null-flags field and MySQL column
   * i is field i + 1.
   * @param slot Slot inside this group.
   * @param out Destination string; the encoded field is appended.
   */
  void AppendCellField(uint32_t field, uint32_t slot, std::string& out) const;

  /**
   * @brief Returns the first cell byte for `field` in this group.
   *
   * @param field Field index, starting with the null-flags field.
   */
  const std::byte* strip(size_t field) const {
    return arena_.get() + strip_offset_[field];
  }

  /**
   * @brief Returns the byte stride between adjacent cells for `field`.
   *
   * @param field Field index, starting with the null-flags field.
   */
  uint32_t stride(size_t field) const { return stride_[field]; }

  /**
   * @brief Returns the schema that defines this group's strip widths.
   */
  const TableSchema& schema() const { return schema_; }

  /**
   * @brief Returns the table store that owns this group.
   */
  PaxStore* store() const { return store_; }

  // Group-level seqlock-style counter for strip-direct readers. Writers bump
  // it once before and once after scattering cells. A reader that observes the
  // same even value before and after scanning saw no completed or in-progress
  // scatter across that interval.
  std::atomic<uint64_t> write_counter{0};

 private:
  const TableSchema& schema_;  // Owned by PaxStore; outlives all groups.
  PaxStore* store_;
  std::vector<uint32_t> stride_;
  std::vector<size_t> strip_offset_;
  std::unique_ptr<std::byte[]> arena_;
  // Slot visibility flags for strip-direct scans.
  std::unique_ptr<std::atomic<uint64_t>[]> visible_;
};

/**
 * @brief Owns all PAX row groups for one LineairDB table.
 *
 * @details `PaxStore` assigns append-only `(group, slot)` locations. It does
 * not publish rows to indexes and does not decide transaction visibility; those
 * remain in the existing LineairDB `DataItem`, Silo, and Masstree layers.
 */
class PaxStore {
 public:
  // 262,144 groups x 8,192 rows = 2^31 slots per table.
  static constexpr size_t kMaxGroups = 1u << 18;

  /**
   * @brief Takes ownership of the table schema used by subsequently allocated
   * groups.
   *
   * @param schema Schema copied into the store and referenced by its groups.
   */
  explicit PaxStore(TableSchema schema);

  /**
   * @brief Allocates the next append-only PAX slot.
   *
   * @details Slots are append-only and are not reused.
   *
   * @return `{nullptr, 0}` when the table has exhausted the fixed directory, so
   * the caller can fall back to heap row storage without losing correctness.
   */
  std::pair<PaxGroup*, uint32_t> AllocateSlot();

  /**
   * @brief Returns the schema used to size every group in this store.
   */
  const TableSchema& schema() const { return schema_; }

  /**
   * @brief Returns group `idx`, or nullptr if it has not been allocated yet.
   *
   * @param idx Group index in the append-only directory.
   */
  PaxGroup* group(size_t idx) const {
    return dir_[idx].load(std::memory_order_acquire);
  }

  /**
   * @brief Returns slots handed out, an upper bound on populated rows.
   */
  uint64_t slots_allocated() const {
    return next_slot_.load(std::memory_order_acquire);
  }

  /**
   * @brief Returns the number of row groups that may contain allocated slots.
   */
  size_t group_count() const {
    const uint64_t slots = slots_allocated();
    return static_cast<size_t>((slots + PaxGroup::kRows - 1) /
                               PaxGroup::kRows);
  }

  /**
   * @brief Records one row that used heap fallback instead of PAX cells.
   */
  void RecordHeapFallback() {
    overflow_count_.fetch_add(1, std::memory_order_relaxed);
  }

  /**
   * @brief Returns the number of rows that used heap fallback.
   */
  uint64_t overflow_count() const {
    return overflow_count_.load(std::memory_order_relaxed);
  }

 private:
  TableSchema schema_;
  std::unique_ptr<std::atomic<PaxGroup*>[]> dir_;
  std::atomic<uint64_t> next_slot_{0};
  std::atomic<uint64_t> overflow_count_{0};
  std::mutex grow_mutex_;
};

}  // namespace Pax
}  // namespace LineairDB

#endif  // LINEAIRDB_PAX_STORE_H
