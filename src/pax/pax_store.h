#ifndef LINEAIRDB_PAX_STORE_H
#define LINEAIRDB_PAX_STORE_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace LineairDB {
namespace Pax {

/**
 * @brief Describes the proxy row fields that a PAX-enabled table stores in
 * strips.
 *
 * @details The proxy row payload is laid out as one null-flags field followed
 * by one field per MySQL column. Each field is encoded as
 * `[byte_size:1][value_length:byte_size little-endian][value_bytes]`, with
 * `byte_size == 0xFF` representing an empty/no-value field. `PaxGroup` uses
 * the maximum payload byte widths here to split a row into fixed-width cells.
 */
struct TableSchema {
  // Max payload bytes per field, starting with the null-flags field.
  std::vector<uint32_t> field_max_bytes;

  /**
   * @brief Returns the number of encoded fields in a proxy row payload.
   */
  size_t field_count() const { return field_max_bytes.size(); }
};

/**
 * @brief Stores a fixed-size row group as per-field PAX strips.
 *
 * @details A `PaxGroup` is the LineairDB-PAX equivalent of a page-sized row
 * group: each logical row occupies the same slot number in every field strip.
 * The row's bytes are stored in the strip cells; `DataItem` continues to own
 * the transaction id word, so Silo validation remains outside this storage
 * class.
 *
 * Concurrency contract: callers invoke `ScatterRow()` only while holding the
 * row's existing Silo TID lock. `GatherRow()` is memory-safe under a concurrent
 * scatter to the same slot, and callers reject torn rows with the normal TID
 * re-check. `write_counter` supports strip-direct readers that bypass per-row
 * TID checks and need to detect group-level writes.
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
  explicit PaxGroup(const TableSchema& schema);

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

  // Group-level seqlock-style counter for strip-direct readers. Writers bump
  // it once before and once after scattering cells. A reader that observes the
  // same even value before and after scanning saw no completed or in-progress
  // scatter across that interval.
  std::atomic<uint64_t> write_counter{0};

 private:
  const TableSchema& schema_;  // Owned by PaxStore; outlives all groups.
  std::vector<uint32_t> stride_;
  std::vector<size_t> strip_offset_;
  std::unique_ptr<std::byte[]> arena_;
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
   * @brief Records one row that used heap fallback instead of PAX cells.
   */
  void BumpOverflow() {
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
