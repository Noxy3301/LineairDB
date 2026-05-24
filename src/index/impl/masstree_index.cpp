#include "masstree_index.hpp"

#include <atomic>
#include <cstring>
#include <mutex>
#include <utility>

// Masstree headers. Must come after the PImpl header guard so other LDB
// sources never see them; masstree's include path is PRIVATE to LDB and
// therefore unreachable from public headers and tests.
#include "config.h"
#include "compiler.hh"
#include "kvthread.hh"
#include "masstree.hh"
#include "masstree_insert.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "masstree_tcursor.hh"
#include "string.hh"

// Globals required by masstree-beta. masstree's kvthread.cc references these
// as externs; exactly one translation unit must define them. Types must
// match kvthread.hh:33-35.
//
// Intentionally left at their initial values: this wrapper uses masstree's
// B+tree structure and nodeversion-based locking for concurrency, but does
// NOT drive masstree's internal RCU machinery. Matches published Silo+
// masstree practice — CCBench's masstree_wrapper.hh and Tu Silo's
// simple_threadinfo stub take the same approach — and has no semantic
// consequences: masstree's insert/scan/remove correctness relies on
// nodeversion bits, not these epochs. Tradeoff: every masstree allocation
// that would otherwise be reclaimed via RCU (overwrite-replaced
// DataItem*, retired leaves/internodes/ksuffix blocks from internal
// splits), plus the entire live tree at process exit (destroy() is
// deliberately not called — see ~Impl()), leaks for the process lifetime.
// Bounded by bench-scope insert churn; not suitable for long-running
// service deployment without a real reclamation path.
relaxed_atomic<mrcu_epoch_type> globalepoch{1};
relaxed_atomic<mrcu_epoch_type> active_epoch{1};
volatile bool recovering = false;

namespace LineairDB {
namespace Index {

namespace {

class key_unparse_unsigned {
 public:
  static int unparse_key(Masstree::key<std::uint64_t> key, char* buf,
                         int buflen) {
    return snprintf(buf, buflen, "%" PRIu64, key.ikey());
  }
};

struct table_params : public Masstree::nodeparams<15, 15> {
  using value_type = DataItem*;
  using value_print_type = Masstree::value_print<value_type>;
  using threadinfo_type = threadinfo;
  using key_unparse_type = key_unparse_unsigned;
  static constexpr ssize_t print_max_indent_depth = 12;
};

using table_type = Masstree::basic_table<table_params>;
using unlocked_cursor_type = Masstree::unlocked_tcursor<table_params>;
using cursor_type = Masstree::tcursor<table_params>;

thread_local threadinfo* tls_ti = nullptr;
// True iff this thread has called rcu_start since its last rcu_stop. Used to
// make ensure_thread_active idempotent within a critical section: re-stamping
// gc_epoch_ on every masstree op would advance the thread past the epoch
// protecting raw DataItem* pointers the caller is still holding (e.g.
// Silo's validation_set_ holds item_p_cache across reads, and a concurrent
// committed delete schedules those DataItems for RCU free). With the gate,
// a thread's gc_epoch_ stays at the epoch of its FIRST masstree access until
// an explicit release at a safe boundary.
thread_local bool tls_enrolled = false;
std::atomic<int> next_thread_id{0};
std::mutex thread_init_mutex;

// threadinfo::make() prepends to masstree's global allthreads list without
// synchronization (kvthread.cc:57-58). Serialize first-use-per-thread to
// avoid UB when multiple worker threads register concurrently. Contended
// only on the first op per thread. Does NOT enrol the thread in the
// current RCU epoch — that is ensure_thread_active's job. Splitting
// registration from enrolment lets short-lived callers (the Database
// construction thread, server-side threads between operations) leave
// gc_epoch_ at 0 so they do not pin min_active_epoch().
inline void ensure_thread_init() {
  if (__builtin_expect(tls_ti == nullptr, 0)) {
    std::lock_guard<std::mutex> lg(thread_init_mutex);
    if (tls_ti == nullptr) {
      tls_ti = threadinfo::make(
          threadinfo::TI_PROCESS,
          next_thread_id.fetch_add(1, std::memory_order_relaxed));
    }
  }
}

// Ensure threadinfo exists AND this thread is enrolled at the current
// masstree epoch — but only if the thread is not already enrolled. This
// idempotency is correctness-critical: every public op uses this, and
// re-stamping gc_epoch_ in the middle of a transaction would let RCU
// reclaim DataItem* pointers the caller is still holding (e.g. Silo
// validation_set_ entries captured from an earlier read). The thread stays
// enrolled at its first-op epoch until an explicit release at a safe
// boundary (MasstreeReleaseThreadEpoch).
inline void ensure_thread_active() {
  ensure_thread_init();
  if (!tls_enrolled) {
    tls_ti->rcu_start();
    tls_enrolled = true;
  }
}

// RCU callback for deferred DataItem deletion. Allocated through
// threadinfo::allocate so it lives in masstree's accounting pool, and
// frees itself in operator() after deleting the wrapped DataItem (the
// same shape as masstree's own gc_layer_rcu_callback, see
// masstree_remove.hh:135-146).
struct DataItemRcuCallback : public threadinfo::mrcu_callback {
  DataItem* item;
  explicit DataItemRcuCallback(DataItem* it) : item(it) {}
  void operator()(threadinfo& ti) override {
    delete item;
    ti.deallocate(this, sizeof(DataItemRcuCallback), memtag_masstree_gc);
  }
};

inline void schedule_data_item_rcu_free(DataItem* item) {
  if (item == nullptr) return;
  void* mem = tls_ti->allocate(sizeof(DataItemRcuCallback), memtag_masstree_gc);
  auto* cb = new (mem) DataItemRcuCallback(item);
  tls_ti->rcu_register(cb);
}

// Adapter that drives masstree's forward/reverse scan into the LDB callback
// shape (operation returning `true` means cancel). When `out_versions` is
// set, every leaf masstree visits records (leaf_ptr, full_version_value) so
// that MasstreeIndex::ValidatePhantoms can re-check at commit.
struct ScanAdapter {
  const char* end_ptr;
  size_t end_len;
  bool has_end;
  std::function<bool(std::string_view)> cb;
  IndexBase* owner;
  std::vector<NodeVersionEntry>* out_versions;
  size_t count = 0;

  template <typename SS, typename K>
  void visit_leaf(const SS& stack, const K&, threadinfo&) {
    if (out_versions == nullptr) return;
    // Use the unlocked projection so the recorded version matches what
    // ValidatePhantoms (and tcursor's next_full_version_value bumping) read
    // back later. Otherwise a stack snapshot taken while some writer briefly
    // held the leaf lock would carry the lock_bit and mismatch.
    out_versions->push_back(
        {owner, static_cast<const void*>(stack.node()),
         static_cast<std::uint64_t>(stack.node()->full_unlocked_version_value())});
  }

  // Returns true to keep scanning, false to stop (masstree convention).
  bool visit_value(Masstree::Str key, DataItem* /*val*/, threadinfo&) {
    if (has_end) {
      const int cmp = std::memcmp(
          end_ptr, key.s,
          std::min(end_len, static_cast<size_t>(key.len)));
      const bool end_greater =
          cmp > 0 ||
          (cmp == 0 &&
           end_len > static_cast<size_t>(key.len));
      if (!end_greater) return false;  // key >= end -> out of range, stop
    }
    ++count;
    if (cb(std::string_view(key.s, key.len))) return false;  // caller cancel
    return true;
  }
};

struct ScanValueAdapter {
  const char* end_ptr;
  size_t end_len;
  bool has_end;
  std::function<bool(std::string_view, DataItem&)> cb;
  IndexBase* owner;
  std::vector<NodeVersionEntry>* out_versions;
  size_t count = 0;

  template <typename SS, typename K>
  void visit_leaf(const SS& stack, const K&, threadinfo&) {
    if (out_versions == nullptr) return;
    // Use the unlocked projection so the recorded version matches what
    // ValidatePhantoms (and tcursor's next_full_version_value bumping) read
    // back later. Otherwise a stack snapshot taken while some writer briefly
    // held the leaf lock would carry the lock_bit and mismatch.
    out_versions->push_back(
        {owner, static_cast<const void*>(stack.node()),
         static_cast<std::uint64_t>(stack.node()->full_unlocked_version_value())});
  }

  bool visit_value(Masstree::Str key, DataItem* val, threadinfo&) {
    if (has_end) {
      const int cmp = std::memcmp(
          end_ptr, key.s,
          std::min(end_len, static_cast<size_t>(key.len)));
      const bool end_greater =
          cmp > 0 ||
          (cmp == 0 &&
           end_len > static_cast<size_t>(key.len));
      if (!end_greater) return false;
    }
    ++count;
    if (cb(std::string_view(key.s, key.len), *val)) return false;
    return true;
  }
};

using leaf_type = Masstree::leaf<table_params>;

}  // namespace

struct MasstreeIndex::Impl {
  table_type table_;

  Impl() {
    // Enrol briefly so table_.initialize can allocate the root node through
    // the threadinfo's RCU-aware allocator, then leave the epoch so the
    // construction thread does not pin min_active_epoch at the initial
    // value forever. If the same thread later does masstree ops, each op
    // path goes through ensure_thread_active and re-enrols.
    //
    // If the caller is ALREADY in an RCU critical section (e.g., a Path B
    // tx on this thread invoked CreateTable / CreateSecondaryIndex
    // mid-transaction), we must not touch its enrolment: rcu_stop()
    // would clear gc_epoch_ while raw DataItem* pointers in tx state are
    // still in use, opening a use-after-free window once another thread's
    // physical-delete schedules them for RCU free. Preserve the section
    // in that case (the existing rcu_start covers the initialize).
    ensure_thread_init();
    const bool was_enrolled = tls_enrolled;
    if (!was_enrolled) {
      tls_ti->rcu_start();
    }
    table_.initialize(*tls_ti);
    if (!was_enrolled) {
      tls_ti->rcu_stop();
    }
  }

  ~Impl() {
    // Do not call table_.destroy(): it schedules RCU free callbacks via
    // deallocate_rcu, but this wrapper never advances the RCU epoch so the
    // callbacks would never run. Consequence: at process exit, the entire
    // live tree (every leaf/internode allocation) and every stored
    // DataItem* leaks. This differs from PL, whose point-index destructor
    // frees stored values. Accepted for benchmark-scope runs only; a real
    // reclamation path is required for long-running service deployment.
  }

  DataItem* Get(std::string_view key) {
    ensure_thread_active();
    unlocked_cursor_type lp(table_, key.data(), key.size());
    if (lp.find_unlocked(*tls_ti)) return lp.value();
    return nullptr;
  }

  // Upsert with a freshly-allocated DataItem. Returns true on success.
  // When `out_update` is non-null and the call structurally bumps the leaf
  // (state=1 = key was absent), records (leaf, prev_version, next_version) so
  // the OCC layer can apply Silo §4.6's own-write node-set rule.
  bool Put(std::string_view key, DataItem&& rhs,
           NodeVersionUpdate* out_update = nullptr) {
    ensure_thread_active();
    auto* fresh = new DataItem(std::move(rhs));
    cursor_type lp(table_, key.data(), key.size());
    bool found = lp.find_insert(*tls_ti);
    if (found) {
      // Overwrite existing slot. Previous DataItem* is leaked: a concurrent
      // reader may still hold the raw pointer returned by Get(). Phase 2b
      // accepts the leak; a Phase 3/4 step will hook this into masstree's
      // RCU limbo list.
      lp.value() = fresh;
    } else {
      lp.value() = fresh;
    }
    if (out_update != nullptr && !found) {
      out_update->node_ptr = static_cast<const void*>(lp.node());
      out_update->old_version =
          static_cast<std::uint64_t>(lp.previous_full_version_value());
      out_update->new_version =
          static_cast<std::uint64_t>(lp.next_full_version_value(1));
      out_update->valid = true;
    }
    fence();
    // 1 == structural insert (bumps the leaf's vinsert counter), 0 == in-place
    // overwrite. Claiming an insert on overwrite would falsely trigger phantom
    // retries on concurrent scanners watching this leaf.
    lp.finish(found ? 0 : 1, *tls_ti);
    return true;
  }

  // Inserts a blank DataItem if absent. Follows PL's EntryState matrix:
  //   EXISTS   -> fail (key already in tree, data initialized)
  //   DELETED  -> succeed (key in tree but data !IsInitialized; reuse slot)
  //   NOT_EXISTS -> succeed (allocate new slot)
  // Must not replace an existing DataItem* on DELETED reuse: concurrent
  // readers may still hold a raw pointer from an earlier Get().
  // When `out_update` is non-null and the call structurally bumps the leaf
  // (NOT_EXISTS branch), records the version delta for Silo §4.6 own-write
  // node-set reconciliation.
  bool Insert(std::string_view key, NodeVersionUpdate* out_update = nullptr) {
    ensure_thread_active();
    cursor_type lp(table_, key.data(), key.size());
    bool found = lp.find_insert(*tls_ti);
    if (found) {
      DataItem* existing = lp.value();
      if (existing != nullptr && existing->IsInitialized()) {
        lp.finish(0, *tls_ti);
        return false;
      }
      if (existing == nullptr) {
        lp.value() = new DataItem();
      }
      fence();
      lp.finish(0, *tls_ti);
      return true;
    }
    if (out_update != nullptr) {
      out_update->node_ptr = static_cast<const void*>(lp.node());
      out_update->old_version =
          static_cast<std::uint64_t>(lp.previous_full_version_value());
      out_update->new_version =
          static_cast<std::uint64_t>(lp.next_full_version_value(1));
      out_update->valid = true;
    }
    lp.value() = new DataItem();
    fence();
    lp.finish(1, *tls_ti);
    return true;
  }

  // Logical delete. Contract must match PL: the entry stays reachable via
  // Get() so that Transaction::Impl::Delete's subsequent Update(nullptr, 0)
  // finds the DataItem and transitions it to !IsInitialized (PL's DELETED
  // state: point-present, range-absent, data !init). A physical erase here
  // would make Get() return nullptr, which Update() interprets as
  // "key missing" and aborts — breaking every primary-key DELETE.
  //
  // The actual structural removal happens later in Purge,
  // called from the OCC layer after the write-set has been installed (i.e.
  // after the DataItem already reads back as !IsInitialized to everyone).
  bool Delete(std::string_view /*key*/) { return true; }

  // Structural removal of a committed delete. Invoked from the OCC backend's
  // install phase while the per-DataItem lock is still held: this is what
  // prevents Insert from observing the freshly-uninitialized slot, racing in
  // a new value, and having the structural erase below silently drop it.
  // Returns true if the key was physically removed; false if the slot is
  // already gone or a racing replacement won the position (defensive — the
  // committed-only contract means we should always find our own DataItem).
  bool Purge(std::string_view key, DataItem* expected) {
    ensure_thread_active();
    cursor_type lp(table_, key.data(), key.size());
    const bool found = lp.find_locked(*tls_ti);
    if (!found) {
      lp.finish(0, *tls_ti);
      return false;
    }
    DataItem* current = lp.value();
    if (expected != nullptr && current != expected) {
      // Some other writer replaced the slot between our commit-time decision
      // and this erase. Leave the new occupant alone.
      lp.finish(0, *tls_ti);
      return false;
    }
    // finish(-1) calls finish_remove which removes the permutation slot and
    // RCU-frees the leaf if it becomes empty. The DataItem* itself rides on
    // a separate RCU callback below.
    lp.finish(-1, *tls_ti);
    schedule_data_item_rcu_free(current);
    return true;
  }

  // Idempotent blank insert: PL's ForcePutBlankEntry never removes, just
  // ensures a slot exists. When `out_update` is non-null and we structurally
  // bumped the leaf, records the version delta for Silo §4.6 own-write
  // node-set reconciliation.
  void ForcePutBlankEntry(std::string_view key,
                           NodeVersionUpdate* out_update = nullptr) {
    ensure_thread_active();
    cursor_type lp(table_, key.data(), key.size());
    bool found = lp.find_insert(*tls_ti);
    if (!found) {
      lp.value() = new DataItem();
    }
    if (out_update != nullptr && !found) {
      out_update->node_ptr = static_cast<const void*>(lp.node());
      out_update->old_version =
          static_cast<std::uint64_t>(lp.previous_full_version_value());
      out_update->new_version =
          static_cast<std::uint64_t>(lp.next_full_version_value(1));
      out_update->valid = true;
    }
    fence();
    lp.finish(found ? 0 : 1, *tls_ti);
  }

  bool EnsureVisibleForSecondaryWrite(
      std::string_view key, NodeVersionUpdate* out_update = nullptr) {
    // Single-tree Masstree has no "range-empty point-present" DELETED state,
    // so any successful insert/idempotent-visit keeps the key observable.
    ForcePutBlankEntry(key, out_update);
    return true;
  }

  std::optional<size_t> Scan(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> op, IndexBase* owner,
      std::vector<NodeVersionEntry>* out_versions) {
    ensure_thread_active();
    ScanAdapter adapter{end.has_value() ? end->data() : nullptr,
                        end.has_value() ? end->size() : 0,
                        end.has_value(),
                        std::move(op),
                        owner,
                        out_versions,
                        0};
    Masstree::Str firstkey(begin.data(), begin.size());
    table_.scan(firstkey, /*emit_firstkey=*/true, adapter, *tls_ti);
    return adapter.count;
  }

  std::optional<size_t> Scan(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem&)> op,
      IndexBase* owner,
      std::vector<NodeVersionEntry>* out_versions) {
    ensure_thread_active();
    ScanValueAdapter adapter{end.data(),
                             end.size(),
                             true,
                             std::move(op),
                             owner,
                             out_versions,
                             0};
    Masstree::Str firstkey(begin.data(), begin.size());
    table_.scan(firstkey, /*emit_firstkey=*/true, adapter, *tls_ti);
    return adapter.count;
  }

  std::optional<size_t> ScanReverse(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> op, IndexBase* owner,
      std::vector<NodeVersionEntry>* out_versions) {
    ensure_thread_active();
    // Reverse scan walks downward from `end - 1`, stopping once key < begin.
    // Range is [begin, end) just like forward Scan.
    auto adapter_op = [b_ptr = begin.data(), b_len = begin.size(),
                       cb = std::move(op)](std::string_view key) mutable {
      const int cmp = std::memcmp(
          b_ptr, key.data(),
          std::min(b_len, key.size()));
      const bool key_below_begin =
          cmp > 0 ||
          (cmp == 0 && b_len > key.size());
      if (key_below_begin) return true;  // below begin -> stop
      return cb(key);
    };
    ScanAdapter adapter{nullptr,
                        0,
                        false,
                        std::move(adapter_op),
                        owner,
                        out_versions,
                        0};
    if (end.has_value()) {
      Masstree::Str firstkey(end->data(), end->size());
      table_.rscan(firstkey, /*emit_firstkey=*/false, adapter, *tls_ti);
    } else {
      // rscan from infinity: use an empty firstkey with emit=true to
      // walk the tail backwards.
      table_.rscan(Masstree::Str(), /*emit_firstkey=*/true, adapter,
                   *tls_ti);
    }
    return adapter.count;
  }

  std::optional<size_t> ScanReverse(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem&)> op,
      IndexBase* owner,
      std::vector<NodeVersionEntry>* out_versions) {
    ensure_thread_active();
    auto adapter_op = [b_ptr = begin.data(), b_len = begin.size(),
                       cb = std::move(op)](std::string_view key,
                                           DataItem& val) mutable {
      const int cmp = std::memcmp(
          b_ptr, key.data(),
          std::min(b_len, key.size()));
      const bool key_below_begin =
          cmp > 0 ||
          (cmp == 0 && b_len > key.size());
      if (key_below_begin) return true;
      return cb(key, val);
    };
    ScanValueAdapter adapter{nullptr,
                             0,
                             false,
                             std::move(adapter_op),
                             owner,
                             out_versions,
                             0};
    Masstree::Str firstkey(end.data(), end.size());
    table_.rscan(firstkey, /*emit_firstkey=*/false, adapter, *tls_ti);
    return adapter.count;
  }

  void ForEach(std::function<bool(std::string_view, DataItem&)> op) {
    ensure_thread_active();
    ScanValueAdapter adapter{nullptr, 0, false, std::move(op),
                             nullptr, nullptr, 0};
    table_.scan(Masstree::Str(), /*emit_firstkey=*/true, adapter, *tls_ti);
  }

  bool ValidatePhantoms(const std::vector<NodeVersionEntry>& entries,
                        IndexBase* self) {
    // Re-stamp gc_epoch_ before dereferencing leaf pointers from `entries`.
    // Those pointers were captured during an earlier scan on this or
    // another thread and are RCU-protected; without an active enrolment
    // this thread could be invisible to min_active_epoch() and let RCU
    // free a leaf out from under us mid-check.
    ensure_thread_active();
    for (const auto& e : entries) {
      if (e.owner != self) continue;  // entry belongs to a different index
      const auto* leaf =
          static_cast<const leaf_type*>(e.node_ptr);
      // full_unlocked_version_value() masks the transient lock_bit (and
      // handles the split-bit corner case), so an unrelated concurrent
      // writer holding the leaf lock at validation time does not produce
      // a false-positive abort. tcursor::previous_full_version_value and
      // next_full_version_value, which seed the entries we are comparing
      // against, also return the unlocked projection -> apples to apples.
      if (static_cast<std::uint64_t>(leaf->full_unlocked_version_value()) !=
          e.version) {
        return false;
      }
    }
    return true;
  }

  void WaitForIndexIsLinearizable() {
    // Masstree's operations are linearizable without explicit fencing on the
    // caller side; unlike PL there is no epoch-deferred event list to drain.
  }
};

MasstreeIndex::MasstreeIndex(Config /*c*/, EpochFramework& /*e*/)
    : impl_(std::make_unique<Impl>()) {}

MasstreeIndex::~MasstreeIndex() = default;

DataItem* MasstreeIndex::Get(std::string_view key) {
  return impl_->Get(key);
}

bool MasstreeIndex::Put(std::string_view key, DataItem&& rhs,
                        NodeVersionUpdate* out_update) {
  bool ok = impl_->Put(key, std::move(rhs), out_update);
  if (out_update != nullptr && out_update->valid) out_update->owner = this;
  return ok;
}

bool MasstreeIndex::Insert(std::string_view key,
                            NodeVersionUpdate* out_update) {
  bool ok = impl_->Insert(key, out_update);
  if (out_update != nullptr && out_update->valid) out_update->owner = this;
  return ok;
}

bool MasstreeIndex::Delete(std::string_view key) {
  return impl_->Delete(key);
}

void MasstreeIndex::ForcePutBlankEntry(std::string_view key,
                                        NodeVersionUpdate* out_update) {
  impl_->ForcePutBlankEntry(key, out_update);
  if (out_update != nullptr && out_update->valid) out_update->owner = this;
}

bool MasstreeIndex::EnsureVisibleForSecondaryWrite(
    std::string_view key, NodeVersionUpdate* out_update) {
  bool ok = impl_->EnsureVisibleForSecondaryWrite(key, out_update);
  if (out_update != nullptr && out_update->valid) out_update->owner = this;
  return ok;
}

std::optional<size_t> MasstreeIndex::Scan(
    std::string_view begin, std::optional<std::string_view> end,
    std::function<bool(std::string_view)> operation,
    std::vector<NodeVersionEntry>* out_versions) {
  return impl_->Scan(begin, end, std::move(operation), this, out_versions);
}

std::optional<size_t> MasstreeIndex::Scan(
    std::string_view begin, std::string_view end,
    std::function<bool(std::string_view, DataItem&)> operation,
    std::vector<NodeVersionEntry>* out_versions) {
  return impl_->Scan(begin, end, std::move(operation), this, out_versions);
}

std::optional<size_t> MasstreeIndex::ScanReverse(
    std::string_view begin, std::optional<std::string_view> end,
    std::function<bool(std::string_view)> operation,
    std::vector<NodeVersionEntry>* out_versions) {
  return impl_->ScanReverse(begin, end, std::move(operation), this,
                            out_versions);
}

std::optional<size_t> MasstreeIndex::ScanReverse(
    std::string_view begin, std::string_view end,
    std::function<bool(std::string_view, DataItem&)> operation,
    std::vector<NodeVersionEntry>* out_versions) {
  return impl_->ScanReverse(begin, end, std::move(operation), this,
                            out_versions);
}

void MasstreeIndex::ForEach(
    std::function<bool(std::string_view, DataItem&)> operation) {
  impl_->ForEach(std::move(operation));
}

void MasstreeIndex::WaitForIndexIsLinearizable() {
  impl_->WaitForIndexIsLinearizable();
}

bool MasstreeIndex::ValidatePhantoms(
    const std::vector<NodeVersionEntry>& entries) {
  return impl_->ValidatePhantoms(entries, this);
}

bool MasstreeIndex::Purge(std::string_view key,
                                            DataItem* expected) {
  return impl_->Purge(key, expected);
}

void MasstreeAdvanceEpoch() {
  // Bump masstree's global epoch and recompute the reclamation watermark.
  // Mirrors mttest.cc's main-thread tick (mttest.cc:124-131). The mutex
  // serialises with ensure_thread_init's threadinfo::make, which prepends
  // to the unsynchronised threadinfo::allthreads list that min_active_epoch
  // walks (kvthread.cc:53-58, kvthread.hh:368-377).
  std::lock_guard<std::mutex> lg(thread_init_mutex);
  globalepoch.store(globalepoch.load() + 1);
  active_epoch.store(threadinfo::min_active_epoch());
}

void MasstreeReleaseThreadEpoch() {
  // End this thread's RCU critical section: drain whatever is eligible
  // under the current active_epoch and clear gc_epoch_. Callers MUST
  // guarantee no raw DataItem* or masstree leaf pointer from inside this
  // section survives past the release — that's the whole point of marking
  // the section closed.
  //
  // This is the hot-path release (called at every RPC safe boundary). It
  // does ONE rcu_stop pass: cheap when limbo is small, and items that
  // remain on the local limbo are drained on the thread's next release
  // when active_epoch has moved further. For a thread that is about to
  // exit and will never call release again, use MasstreeFullyDrainThread
  // instead.
  if (tls_ti == nullptr) return;
  tls_ti->rcu_stop();
  tls_enrolled = false;
}

void MasstreeFullyDrainThread() {
  // Best-effort drain of the calling thread's local limbo before it
  // exits. Strategy: leave the min_active_epoch participant set first
  // (rcu_stop sets gc_epoch_=0 and frees the first eligible batch), then
  // bump the global epoch and re-call rcu_stop to peel further 128-entry
  // batches.
  //
  // KNOWN LIMITATION (cross-thread limbo leak): this is best-effort, not
  // a guarantee. Two conditions can permanently strand entries on this
  // thread's local limbo after it exits:
  //
  //   (a) Some OTHER thread holds an old gc_epoch_ that pins active_epoch
  //       below the epoch our items were retired at. Our items remain
  //       ineligible; the loop bumps globalepoch but min_active_epoch
  //       stays low.
  //   (b) We retired more entries than 4096 * 128 = 512K within this
  //       section. The loop cap exits before they are all drained.
  //
  // After the detached connection thread exits, its threadinfo lingers
  // on masstree's allthreads list but no future call runs on it. The
  // limbo is per-threadinfo and only the owning thread can safely drain
  // it via masstree's API, so the stranded items leak for the process
  // lifetime.
  //
  // Closing this properly requires either (i) a server architecture
  // change to keep RPC threads alive (thread pool instead of detached
  // per-connection threads) so the same thread eventually drains its
  // own limbo on a later op, or (ii) patching masstree-beta to expose a
  // cross-thread drain primitive so the LineairDB epoch ticker can
  // process exited threadinfos. Both are out of scope for this patch.
  //
  // In the meantime: the leak is bounded by per-connection-close
  // workload. Benchmarks that keep connections alive (BenchBase / sysbench
  // style pools) do not trigger it. Long-running services with high
  // connection churn under contended workloads will accumulate it.
  //
  // Not for the hot path: each MasstreeAdvanceEpoch acquires
  // thread_init_mutex and re-walks the allthreads list, so calling this
  // on every RPC boundary causes contention collapse at high
  // concurrency.
  if (tls_ti == nullptr) return;
  tls_ti->rcu_stop();
  tls_enrolled = false;
  for (int i = 0; i < 4096; ++i) {
    MasstreeAdvanceEpoch();
    tls_ti->rcu_stop();
  }
}

}  // namespace Index
}  // namespace LineairDB
