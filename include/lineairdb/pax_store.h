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
 * exactly like the row-store's in-place overwrite discipline.
 *
 * Strip-direct readers (the server's unvalidated scan fast path) bypass
 * per-row TIDs entirely. Their visibility rules:
 *   - a slot is only readable when its visibility bit is set; the bit is set
 *     after a successful scatter (commit install) and cleared by tombstone
 *     installs, so unallocated / never-installed / deleted slots are
 *     invisible;
 *   - the per-group write_counter is bumped at modification start and end;
 *     a reader that observes an unchanged counter around its pass over a
 *     group saw no concurrent modification begin or complete, so its cell
 *     reads are consistent; otherwise it retries the group;
 *   - a table whose rows ever fell back to the heap (overflow_count() > 0)
 *     must not be strip-scanned at all — heap rows are invisible to strips.
 */
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

struct TableSchema {
  // Max payload bytes per field, index 0 = the null-flags field, then one
  // entry per MySQL column in field order. Cells store [u16 len][payload].
  std::vector<uint32_t> field_max_bytes;
  size_t field_count() const { return field_max_bytes.size(); }
};

class PaxStore;

class PaxGroup {
 public:
  static constexpr uint32_t kRows = 8192;
  static constexpr uint32_t kCellLenBytes = 2;  // u16 length prefix per cell

  PaxGroup(const TableSchema& schema, PaxStore* store);

  // Shred one row (proxy row format) into this group's strips and mark the
  // slot visible. Returns false (with the slot left invisible) when the row
  // does not match the schema shape or a cell exceeds its declared width.
  bool ScatterRow(uint32_t slot, const std::byte* row, size_t size);

  // Tombstone install: hide the slot from strip-direct readers.
  void RetireSlot(uint32_t slot);

  // Reconstruct the row into dst (exactly `expected_size` bytes, which the
  // caller tracks in DataBuffer::size). May produce garbage under a
  // concurrent ScatterRow to the same slot; caller's TID re-check rejects
  // that case. Returns bytes written (== expected_size on a quiet slot).
  size_t GatherRow(uint32_t slot, std::byte* dst, size_t expected_size) const;

  // Reconstruct only the null-flags field plus the given columns (0-based,
  // strictly ascending) — the same trimmed layout the server's
  // trim_row_value produces. Appends to `out`. Returns false when a column
  // index is outside the schema.
  bool GatherRowProjected(uint32_t slot, const uint32_t* columns,
                          size_t n_columns, std::string& out) const;

  // Reconstruct the full row SHAPE but with real payloads only for the
  // given columns (0-based, strictly ascending); every other column becomes
  // the 1-byte empty field (0xFF). Field headers stay parseable and column
  // indexes stay stable, so filters/projections that only touch the listed
  // columns behave exactly as on the full row while the gather touches only
  // those strips. Appends to `out`.
  void GatherRowSparse(uint32_t slot, const uint32_t* columns,
                       size_t n_columns, std::string& out) const;

  bool IsVisible(uint32_t slot) const {
    return (visible_[slot >> 6].load(std::memory_order_acquire) >>
            (slot & 63)) &
           1u;
  }

  // Strip-direct cell access (server scan fast path). `field` 0 is the
  // null-flags field; MySQL column i is field i+1. The returned view's
  // length is clamped to the declared width (memory-safe under races).
  std::string_view cell(size_t field, uint32_t slot) const {
    const std::byte* c = arena_.get() + strip_offset_[field] +
                         static_cast<size_t>(stride_[field]) * slot;
    uint16_t len;
    std::memcpy(&len, c, sizeof(len));
    if (len > schema_.field_max_bytes[field]) len = 0;
    return std::string_view(reinterpret_cast<const char*>(c) + kCellLenBytes,
                            len);
  }

  const std::byte* strip(size_t field) const {
    return arena_.get() + strip_offset_[field];
  }
  uint32_t stride(size_t field) const { return stride_[field]; }
  const TableSchema& schema() const { return schema_; }
  PaxStore* store() const { return store_; }

  // See file header: bumped once at modification start and once at end.
  std::atomic<uint64_t> write_counter{0};

 private:
  const TableSchema& schema_;  // owned by PaxStore; outlives all groups
  PaxStore* store_;
  std::vector<uint32_t> stride_;
  std::vector<size_t> strip_offset_;
  std::unique_ptr<std::byte[]> arena_;
  std::unique_ptr<std::atomic<uint64_t>[]> visible_;  // kRows bits
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
  size_t group_count() const {
    const uint64_t slots = slots_allocated();
    return static_cast<size_t>((slots + PaxGroup::kRows - 1) /
                               PaxGroup::kRows);
  }

  // Rows that did not fit their declared cell widths and permanently fell
  // back to the heap path. Non-zero disables strip-direct scans for this
  // table (those rows would be invisible to strips).
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
