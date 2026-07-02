/*
 * PAX single-copy storage: per-table row groups holding column strips.
 *
 * A row's payload bytes (the proxy row format: [null_field][col_0]...[col_n],
 * each field = [byteSize:1][valueLength:byteSize LE][value]) are shredded
 * into per-field fixed-width cells so that server-side scans get column
 * locality, while DataItem keeps owning the transaction_id word (Silo CC is
 * untouched). Rows that do not fit their declared cell widths fall back to
 * the row-store heap path at the caller (DataBuffer), so correctness never
 * depends on schema width guesses.
 *
 * Concurrency contract: ScatterRow is only called while the row's TID lock
 * bit is held (the existing Silo install discipline), so per-slot writes
 * never race with each other. GatherRow may race with a concurrent
 * ScatterRow to the same slot; it is memory-safe (lengths are clamped to the
 * cell width) and the torn result is rejected by the caller's TID re-check,
 * exactly like the row-store's in-place overwrite discipline. The per-group
 * write counter exists for the (later) strip-direct scan path: readers that
 * bypass per-row TIDs compare the counter before/after scanning a group.
 */
#ifndef LINEAIRDB_PAX_STORE_H
#define LINEAIRDB_PAX_STORE_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <vector>

namespace LineairDB {
namespace Pax {

struct TableSchema {
  // Max payload bytes per field, index 0 = the null-flags field, then one
  // entry per MySQL column in field order. Cells store [u16 len][payload].
  std::vector<uint32_t> field_max_bytes;
  size_t field_count() const { return field_max_bytes.size(); }
};

class PaxGroup {
 public:
  static constexpr uint32_t kRows = 8192;
  static constexpr uint32_t kCellLenBytes = 2;  // u16 length prefix per cell

  explicit PaxGroup(const TableSchema& schema);

  // Shred one row (proxy row format) into this group's strips.
  // Returns false (without partial writes) when the row does not match the
  // schema shape or a cell exceeds its declared width.
  bool ScatterRow(uint32_t slot, const std::byte* row, size_t size);

  // Reconstruct the row into dst (exactly `expected_size` bytes, which the
  // caller tracks in DataBuffer::size). May produce garbage under a
  // concurrent ScatterRow to the same slot; caller's TID re-check rejects
  // that case. Returns bytes written (== expected_size on a quiet slot).
  size_t GatherRow(uint32_t slot, std::byte* dst, size_t expected_size) const;

  const std::byte* strip(size_t field) const {
    return arena_.get() + strip_offset_[field];
  }
  uint32_t stride(size_t field) const { return stride_[field]; }
  const TableSchema& schema() const { return schema_; }

  // Write counter for strip-direct readers: bumped once at scatter start and
  // once at scatter end; a reader observing an unchanged value saw no
  // concurrent scatter complete or begin in between.
  std::atomic<uint64_t> write_counter{0};

 private:
  const TableSchema& schema_;  // owned by PaxStore; outlives all groups
  std::vector<uint32_t> stride_;
  std::vector<size_t> strip_offset_;
  std::unique_ptr<std::byte[]> arena_;
};

class PaxStore {
 public:
  // 262,144 groups x 8,192 rows = 2^31 slots per table.
  static constexpr size_t kMaxGroups = 1u << 18;

  explicit PaxStore(TableSchema schema);

  // Allocate the next slot (append-only; slots are never reused).
  // Returns {nullptr, 0} when the table is full — caller falls back to heap.
  std::pair<PaxGroup*, uint32_t> AllocateSlot();

  const TableSchema& schema() const { return schema_; }

  PaxGroup* group(size_t idx) const {
    return dir_[idx].load(std::memory_order_acquire);
  }
  // Number of slots handed out (upper bound on populated slots).
  uint64_t slots_allocated() const {
    return next_slot_.load(std::memory_order_acquire);
  }

  void BumpOverflow() {
    overflow_count_.fetch_add(1, std::memory_order_relaxed);
  }
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
