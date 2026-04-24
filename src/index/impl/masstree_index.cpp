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
std::atomic<int> next_thread_id{0};
std::mutex thread_init_mutex;

// threadinfo::make() prepends to masstree's global allthreads list without
// synchronization (kvthread.cc:57-58). Serialize first-use-per-thread to
// avoid UB when multiple worker threads register concurrently. Contended
// only on the first op per thread.
inline void ensure_thread_init() {
  if (__builtin_expect(tls_ti == nullptr, 0)) {
    std::lock_guard<std::mutex> lg(thread_init_mutex);
    tls_ti = threadinfo::make(
        threadinfo::TI_PROCESS,
        next_thread_id.fetch_add(1, std::memory_order_relaxed));
  }
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
    out_versions->push_back(
        {owner, static_cast<const void*>(stack.node()),
         static_cast<std::uint64_t>(stack.full_version_value())});
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
    out_versions->push_back(
        {owner, static_cast<const void*>(stack.node()),
         static_cast<std::uint64_t>(stack.full_version_value())});
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
    ensure_thread_init();
    table_.initialize(*tls_ti);
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
    ensure_thread_init();
    unlocked_cursor_type lp(table_, key.data(), key.size());
    if (lp.find_unlocked(*tls_ti)) return lp.value();
    return nullptr;
  }

  // Upsert with a freshly-allocated DataItem. Returns true on success.
  bool Put(std::string_view key, DataItem&& rhs) {
    ensure_thread_init();
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
    fence();
    lp.finish(1, *tls_ti);
    return true;
  }

  // Inserts a blank DataItem if absent. Follows PL's EntryState matrix:
  //   EXISTS   -> fail (key already in tree, data initialized)
  //   DELETED  -> succeed (key in tree but data !IsInitialized; reuse slot)
  //   NOT_EXISTS -> succeed (allocate new slot)
  // Must not replace an existing DataItem* on DELETED reuse: concurrent
  // readers may still hold a raw pointer from an earlier Get().
  bool Insert(std::string_view key) {
    ensure_thread_init();
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
  // Range-visibility for concurrent scanners is caught at Precommit via
  // deferred ValidatePhantoms (structural check) and Silo's read-set TID
  // validation (DataItem mutation check). PL detects the same conflicts
  // synchronously via IsInPredicateSet.
  bool Delete(std::string_view /*key*/) { return true; }

  // Idempotent blank insert: PL's ForcePutBlankEntry never removes, just
  // ensures a slot exists.
  void ForcePutBlankEntry(std::string_view key) {
    ensure_thread_init();
    cursor_type lp(table_, key.data(), key.size());
    bool found = lp.find_insert(*tls_ti);
    if (!found) {
      lp.value() = new DataItem();
    }
    fence();
    lp.finish(found ? 0 : 1, *tls_ti);
  }

  bool EnsureVisibleForSecondaryWrite(std::string_view key) {
    // Single-tree Masstree has no "range-empty point-present" DELETED state,
    // so any successful insert/idempotent-visit keeps the key observable.
    ForcePutBlankEntry(key);
    return true;
  }

  std::optional<size_t> Scan(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> op, IndexBase* owner,
      std::vector<NodeVersionEntry>* out_versions) {
    ensure_thread_init();
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
    ensure_thread_init();
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
    ensure_thread_init();
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
    ensure_thread_init();
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
    ensure_thread_init();
    ScanValueAdapter adapter{nullptr, 0, false, std::move(op),
                             nullptr, nullptr, 0};
    table_.scan(Masstree::Str(), /*emit_firstkey=*/true, adapter, *tls_ti);
  }

  bool ValidatePhantoms(const std::vector<NodeVersionEntry>& entries,
                        IndexBase* self) {
    for (const auto& e : entries) {
      if (e.owner != self) continue;  // entry belongs to a different index
      const auto* leaf =
          static_cast<const leaf_type*>(e.node_ptr);
      if (static_cast<std::uint64_t>(leaf->full_version_value()) !=
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

bool MasstreeIndex::Put(std::string_view key, DataItem&& rhs) {
  return impl_->Put(key, std::move(rhs));
}

bool MasstreeIndex::Insert(std::string_view key) {
  return impl_->Insert(key);
}

bool MasstreeIndex::Delete(std::string_view key) {
  return impl_->Delete(key);
}

void MasstreeIndex::ForcePutBlankEntry(std::string_view key) {
  impl_->ForcePutBlankEntry(key);
}

bool MasstreeIndex::EnsureVisibleForSecondaryWrite(std::string_view key) {
  return impl_->EnsureVisibleForSecondaryWrite(key);
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

}  // namespace Index
}  // namespace LineairDB
