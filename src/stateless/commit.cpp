#include "stateless/commit.h"

#include <algorithm>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "index/concurrent_table.h"
#include "index/reaper.h"
#include "index/secondary_index.h"
#include "pax/version_store.hpp"
#include "recovery/logger.h"
#include "stateless/packed_transaction_id.hpp"
#include "table/table.h"
#include "table/table_dictionary.hpp"
#include "types/data_item.hpp"
#include "util/debug_sync.hpp"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Stateless {

bool Commit(TableDictionary& tables, std::shared_mutex& schema_mutex,
            EpochFramework& epoch_framework, Index::Reaper& reaper,
            Recovery::Logger& logger, const Config& config,
            const std::vector<ExternalReadEntry>& reads,
            const std::vector<ExternalWriteEntry>& writes,
            const std::vector<ExternalSecondaryIndexEntry>& secondary_index_ops,
            const std::vector<ExternalRangeReadEntry>& range_reads,
            std::string* abort_reason) {
  // Epoch join.
  epoch_framework.MakeMeOnline();
  if (abort_reason != nullptr) abort_reason->clear();

  struct ValidationEntry {
    Table* table = nullptr;
    std::string table_name;
    std::string key;
    DataItem* item = nullptr;
    TransactionId captured_tid;
    bool found = false;
  };
  struct ResolvedWrite {
    std::string table_name;
    std::string key;
    std::string value;
    bool is_delete = false;
    DataItem* item = nullptr;
    Index::ConcurrentTable* index = nullptr;
  };
  struct ResolvedSecondaryIndexOp {
    std::string table_name;
    std::string index_name;
    std::string secondary_key;
    std::string primary_key;
    bool is_delete = false;
    DataItem* item = nullptr;
    Index::SecondaryIndex* index = nullptr;
    Index::SecondaryIndexType index_type;
  };
  struct LockTarget {
    DataItem* item = nullptr;
    Index::ConcurrentTable* primary_index = nullptr;
    Index::SecondaryIndex* secondary_index = nullptr;
    std::string key;
  };

  std::vector<ValidationEntry> validation_entries;
  std::vector<ResolvedWrite> resolved_writes;
  std::vector<ResolvedSecondaryIndexOp> resolved_si_ops;
  std::vector<DataItem*> lock_items;
  std::vector<LockTarget> lock_targets;
  std::unordered_set<std::string> unique_si_adds;

  auto abort_before_lock = [&](const std::string& reason) {
    if (abort_reason != nullptr) *abort_reason = reason;
    epoch_framework.MakeMeOffline();
    return false;
  };

  // A range entry without its exclusive end bound cannot be replayed;
  // abort instead of skipping the validation.
  for (const auto& range : range_reads) {
    if (range.end_key.empty()) {
      return abort_before_lock("range_end_key_missing");
    }
  }

  // Resolve (R1-R2): map reads, writes, and SI ops to their DataItems.
  {
    std::shared_lock<std::shared_mutex> lk(schema_mutex);

    // Resolve point reads to the DataItem and version observed by proxy
    validation_entries.reserve(reads.size());
    for (const auto& read : reads) {
      auto table = tables.GetTable(read.table_name);
      if (!table.has_value()) {
        if (read.found || read.tid != 0) {
          return abort_before_lock("read_table_missing");
        }
        continue;
      }

      DataItem* item = table.value()->GetPrimaryIndex().Get(read.key);
      validation_entries.push_back({table.value(), read.table_name, read.key,
                                    item, UnpackTransactionId(read.tid),
                                    read.found});
    }

    // Resolve row writes and deletes to the primary-index entries to lock.
    // R2: a key with no DataItem yet cannot be locked, so GetOrInsert
    // materializes a blank slot (uninitialized, TID 0); this mutates the
    // tree but takes no row lock — Silo's native insert stages an "absent"
    // record the same way.
    resolved_writes.reserve(writes.size());
    for (const auto& write : writes) {
      auto table = tables.GetTable(write.table_name);
      if (!table.has_value()) {
        return abort_before_lock("write_table_missing");
      }

      DataItem* item =
          table.value()->GetPrimaryIndex().GetOrInsert(write.key);
      if (item == nullptr) {
        return abort_before_lock("write_get_or_insert_failed");
      }

      auto* primary_index = &table.value()->GetPrimaryIndex();
      resolved_writes.push_back({write.table_name, write.key, write.value,
                                 write.is_delete, item, primary_index});
      lock_items.push_back(item);
      lock_targets.push_back({item, primary_index, nullptr, write.key});
    }

    // Resolve secondary-index updates to the secondary-index entries to lock
    resolved_si_ops.reserve(secondary_index_ops.size());
    for (const auto& op : secondary_index_ops) {
      auto table = tables.GetTable(op.table_name);
      if (!table.has_value()) {
        return abort_before_lock("si_table_missing");
      }

      Index::SecondaryIndex* index =
          table.value()->GetSecondaryIndex(op.index_name);
      if (index == nullptr) {
        return abort_before_lock("si_index_missing");
      }

      // R3: reject the same UNIQUE SI key appearing twice in this request.
      if (!op.is_delete && index->IsUnique()) {
        const std::string unique_key =
            op.table_name + '\0' + op.index_name + '\0' + op.secondary_key;
        if (!unique_si_adds.insert(unique_key).second) {
          return abort_before_lock("unique_si_duplicate_in_request");
        }
      }

      DataItem* item = nullptr;
      if (op.is_delete) {
        item = index->GetOrInsert(op.secondary_key);
      } else {
        item = index->GetOrInsertForWrite(op.secondary_key);
      }
      if (item == nullptr) {
        return abort_before_lock("si_get_or_insert_failed");
      }

      resolved_si_ops.push_back({op.table_name, op.index_name,
                                 op.secondary_key, op.primary_key,
                                 op.is_delete, item, index,
                                 index->GetIndexType()});
      lock_items.push_back(item);
      lock_targets.push_back({item, nullptr, index, op.secondary_key});
    }
  }

  // Phase 1.1: address-sort and CAS-lock every write target. One global
  // lock order keeps concurrent committers free of write-write deadlock;
  // the pre-lock TID is kept because validation must compare reads against
  // it, not against the TID we just dirtied.
  std::sort(lock_items.begin(), lock_items.end());
  lock_items.erase(std::unique(lock_items.begin(), lock_items.end()),
                   lock_items.end());

  struct LockedTid {
    DataItem* item = nullptr;
    TransactionId before_lock;
    TransactionId locked;
  };

  std::vector<LockedTid> locked_tids;
  locked_tids.reserve(lock_items.size());

  auto unlock_and_abort = [&](const std::string& reason) {
    if (abort_reason != nullptr) *abort_reason = reason;
    for (auto& locked : locked_tids) {
      TransactionId current = locked.item->transaction_id.load();
      if (current.tid & 1u) {
        current.tid--;
        locked.item->transaction_id.store(current);
      }
    }
    epoch_framework.MakeMeOffline();
    return false;
  };

  auto lock_target_attached = [&](DataItem* item) {
    for (const auto& target : lock_targets) {
      if (target.item != item) continue;
      DataItem* current = nullptr;
      if (target.primary_index != nullptr) {
        current = target.primary_index->Get(target.key);
      } else if (target.secondary_index != nullptr) {
        current = target.secondary_index->Get(target.key);
      } else {
        continue;
      }
      if (current != item) return false;
    }
    return true;
  };

  // Lock loop: spin until the LSB CAS lands.
  for (auto* item : lock_items) {
    for (;;) {
      TransactionId current = item->transaction_id.load();
      if (current.tid & 1u) {
        _mm_pause();
        continue;
      }
      TransactionId locked = current;
      locked.tid |= 1u;
      if (item->transaction_id.compare_exchange_weak(current, locked)) {
        locked_tids.push_back({item, current, locked});
        if (!lock_target_attached(item)) {
          return unlock_and_abort("write_target_detached");
        }
        break;
      }
    }
  }

  // Phase 1.2: re-read the global epoch with all locks held — the
  // serialization point: epoch-grouped commit and recovery follow the
  // serial order only if the commit epoch is taken here. The thread-local
  // epoch is fixed at join time, so leaving and re-joining is the only way
  // to re-read it.
  epoch_framework.MakeMeOffline();
  epoch_framework.MakeMeOnline();

  // Phase 2.1: re-read exact-key TIDs and confirm they have not moved; a
  // moved TID means a concurrent commit overwrote the row after the caller
  // read it.
  auto key_hex = [](const std::string& key) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(key.size() * 2);
    for (unsigned char byte : key) {
      out.push_back(kHex[byte >> 4]);
      out.push_back(kHex[byte & 0x0F]);
    }
    return out;
  };

  auto exact_read_reason = [&](const char* reason,
                               const ValidationEntry& read) {
    std::string out(reason);
    out += ':';
    out += read.table_name;
    out += ":key=";
    out += key_hex(read.key);
    return out;
  };

  for (const auto& read : validation_entries) {
    DataItem* item = read.table->GetPrimaryIndex().Get(read.key);
    if (item == nullptr) {
      // A read observed as present must still resolve at validation
      // time. An unresolvable key here means a committed delete purged
      // the slot after the read, which is a serializability conflict.
      if (read.found) {
        return unlock_and_abort(
            exact_read_reason("exact_read_disappeared", read));
      }
      continue;
    }

    if (!read.found) {
      if (!item->IsPrimaryInitialized()) {
        continue;
      }
      return unlock_and_abort(
          exact_read_reason("exact_read_appeared", read));
    }

    TransactionId expected = read.captured_tid;
    for (const auto& locked : locked_tids) {
      if (locked.item == item) {
        if (locked.before_lock.epoch != read.captured_tid.epoch ||
            locked.before_lock.tid != read.captured_tid.tid) {
          return unlock_and_abort(
              exact_read_reason("exact_read_tid_moved", read));
        }
        expected = locked.locked;
        break;
      }
    }

    if (item->transaction_id.load() != expected) {
      return unlock_and_abort(
          exact_read_reason("exact_read_tid_moved", read));
    }
    if (read.found && !item->IsPrimaryInitialized()) {
      return unlock_and_abort(
          exact_read_reason("exact_read_deleted", read));
    }
  }

  // Phase 2.2: replay each range scan and compare the key lists. This is
  // the phantom check Silo performs with Masstree node versions (physical
  // validation), done by value because a stateless caller cannot hold node
  // pointers across the RPC boundary. The comparison is membership only;
  // row TIDs are validated at 2.1.
  auto is_own_locked = [&](DataItem* item) {
    for (const auto& locked : locked_tids) {
      if (locked.item == item) return true;
    }
    return false;
  };

  // Silo Phase 2 read validation is wait-free: a record locked by another
  // transaction is treated as dirty and forces abort. Spinning here breaks
  // the paper's deadlock-freedom invariant — sorted write-lock acquisition
  // protects only write-to-write edges, not read-to-write edges introduced
  // by validators.
  auto locked_by_another = [&](DataItem* item) {
    TransactionId tid = item->transaction_id.load();
    if (!(tid.tid & 1u)) return false;
    return !is_own_locked(item);
  };

  auto validate_primary_key_list =
      [&](const ExternalRangeReadEntry& range) {
        auto table = tables.GetTable(range.table_name);
        if (!table.has_value()) return false;

        // Compare positionally against the evidence and stop at the first
        // divergence, which bounds the replay at one live row past the
        // evidence even when the range is unlimited.
        size_t result_pos = 0;
        bool aborted = false;
        bool matches = true;
        auto collect_key = [&](std::string_view key, DataItem& item) {
          if (locked_by_another(&item)) {
            aborted = true;
            return true;
          }
          if (item.IsPrimaryInitialized()) {
            if (result_pos >= range.result_keys.size() ||
                std::string_view(range.result_keys[result_pos]) != key) {
              matches = false;
              return true;
            }
            ++result_pos;
          }
          return range.row_limit > 0 && result_pos >= range.row_limit;
        };

        auto scan_result =
            range.reverse_scan
                ? table.value()->GetPrimaryIndex().ScanReverse(
                      range.start_key, range.end_key, collect_key, nullptr)
                : table.value()->GetPrimaryIndex().Scan(
                      range.start_key, range.end_key, collect_key, nullptr);
        if (aborted) return false;
        return scan_result.has_value() && matches &&
               result_pos == range.result_keys.size();
      };

  auto validate_secondary_key_list =
      [&](const ExternalRangeReadEntry& range) {
        auto table = tables.GetTable(range.table_name);
        if (!table.has_value()) return false;
        auto* index = table.value()->GetSecondaryIndex(range.index_name);
        if (index == nullptr) return false;

        size_t result_pos = 0;
        bool aborted = false;
        bool matches = true;
        auto collect_base_row = [&](const std::string& secondary_key,
                                    std::string_view primary_key) {
          DataItem* item = table.value()->GetPrimaryIndex().Get(primary_key);
          if (item == nullptr) return false;
          if (locked_by_another(item)) {
            aborted = true;
            return true;
          }
          if (item->IsPrimaryInitialized()) {
            if (result_pos >= range.result_keys.size() ||
                result_pos >= range.result_primary_keys.size() ||
                std::string_view(range.result_keys[result_pos]) !=
                    std::string_view(secondary_key) ||
                std::string_view(range.result_primary_keys[result_pos]) !=
                    primary_key) {
              matches = false;
              return true;
            }
            ++result_pos;
          }
          return range.row_limit > 0 &&
                 result_pos >= range.row_limit;
        };

        auto collect_secondary_key = [&](std::string_view key) {
          const std::string secondary_key(key);
          DataItem* item = index->Get(key);
          if (item == nullptr) return false;
          // Pin the immutable primary-key list under a double-TID read, as in
          // the staging scan. A committer publishes a new list under its lock;
          // the TID stays constant while own-locked, so re-check after loading.
          const TransactionId observed = item->transaction_id.load();
          if ((observed.tid & 1u) && !is_own_locked(item)) {
            aborted = true;
            return true;
          }
          auto primary_keys = std::atomic_load(&item->primary_keys_);
          const bool secondary_live =
              primary_keys && primary_keys->count != 0;
          if (item->transaction_id.load() != observed) {
            aborted = true;
            return true;
          }
          if (!secondary_live) {
            return false;
          }
          for (std::string_view primary_key :
               PackedPrimaryKeysView(primary_keys)) {
            if (collect_base_row(secondary_key, primary_key)) return true;
          }
          return false;
        };

        auto scan_result =
            range.reverse_scan
                ? index->ScanReverse(range.start_key, range.end_key,
                                     collect_secondary_key, nullptr)
                : index->Scan(range.start_key, range.end_key,
                              collect_secondary_key, nullptr);
        if (aborted) return false;
        return scan_result.has_value() && matches &&
               result_pos == range.result_keys.size() &&
               result_pos == range.result_primary_keys.size();
      };

  for (const auto& range : range_reads) {
    const bool ok = range.index_name.empty()
                        ? validate_primary_key_list(range)
                        : validate_secondary_key_list(range);
    if (!ok) {
      return unlock_and_abort(range.index_name.empty()
                                  ? "primary_range_result_changed"
                                  : "secondary_range_result_changed");
    }
  }

  // Phase 2.3: post-lock UNIQUE recheck. A competing add may have installed
  // the same secondary key while we were waiting on the write lock, so the
  // resolve-time dedup (R3) is not enough on its own.
  std::unordered_map<DataItem*, PackedPrimaryKeys::Ptr> si_primary_keys;
  for (const auto& op : resolved_si_ops) {
    if (!op.index_type.IsUnique()) continue;

    auto [state_it, inserted] =
        si_primary_keys.emplace(op.item, PackedPrimaryKeys::Ptr{});
    if (inserted) {
      state_it->second = std::atomic_load(&op.item->primary_keys_);
    }
    auto& primary_keys = state_it->second;
    const PackedPrimaryKeysView keys(primary_keys);

    auto key_it = keys.lower_bound(op.primary_key);
    const bool key_exists =
        key_it != keys.end() && *key_it == std::string_view(op.primary_key);

    if (op.is_delete) {
      if (key_exists) {
        primary_keys = PackedPrimaryKeys::Erase(primary_keys, op.primary_key);
      }
      continue;
    }

    if (!keys.empty()) {
      return unlock_and_abort("unique_si_exists_after_lock");
    }
    primary_keys = PackedPrimaryKeys::Insert(primary_keys, op.primary_key);
  }

  // Phase 3.1: install row writes/deletes and SI add/remove. Deletes leave
  // tombstones in the tree; physical removal is deferred until a later
  // epoch so same-key reinserts reuse the slot and advance its TID chain.
  {
    // Tags the install region with the commit epoch so the PAX
    // before-image capture can label its entries.
    Pax::ScopedCommitEpoch commit_epoch_scope(
        epoch_framework.GetMyThreadLocalEpoch());
    size_t installed = 0;
    for (auto& write : resolved_writes) {
      if (installed > 0) {
        LINEAIRDB_DEBUG_SYNC("stateless_commit.between_row_installs");
      }
      if (write.is_delete) {
        write.item->Reset(nullptr, 0);
      } else {
        write.item->Reset(
            reinterpret_cast<const std::byte*>(write.value.data()),
            write.value.size());
      }
      ++installed;
    }
  }

  // Install secondary-index add/remove. Empty SI slots are tombstones too;
  // their physical removal is deferred with primary rows.
  for (auto& op : resolved_si_ops) {
    const auto* primary_key =
        reinterpret_cast<const std::byte*>(op.primary_key.data());
    if (op.is_delete) {
      op.item->RemoveSecondaryIndexValue(primary_key,
                                         op.primary_key.size());
    } else {
      op.item->AddSecondaryIndexValue(primary_key, op.primary_key.size());
    }
  }

  // Capture SI tombstone state while the slots are still locked. The live
  // primary-key list pointer must not be read after unlock: a concurrent
  // committer can publish a replacement under its own lock.
  // Computed after the whole install loop so a delete-then-add sequence on
  // the same slot within this transaction reads the final state.
  std::unordered_map<DataItem*, bool> si_empty_after_install;
  for (const auto& op : resolved_si_ops) {
    if (!op.is_delete) continue;
    const auto primary_keys = std::atomic_load(&op.item->primary_keys_);
    si_empty_after_install[op.item] =
        PackedPrimaryKeysView(primary_keys).empty();
  }

  // Phase 3.2: build the log snapshot before unlock so a later transaction
  // cannot overwrite the values we just logged.
  WriteSetType log_set;
  bool has_log_set = false;
  if (config.enable_logging) {
    has_log_set = true;
    log_set.reserve(resolved_writes.size() + resolved_si_ops.size());

    for (const auto& write : resolved_writes) {
      Snapshot snapshot(write.key, nullptr, 0, write.item, write.table_name,
                        "");
      snapshot.data_item_copy = *write.item;
      log_set.emplace_back(std::move(snapshot));
    }
    for (const auto& op : resolved_si_ops) {
      Snapshot snapshot(op.secondary_key, nullptr, 0, op.item,
                        op.table_name, op.index_name, 0, op.index_type);
      snapshot.data_item_copy = *op.item;
      snapshot.RecordSecondaryIndexDelta(
          op.primary_key,
          op.is_delete ? SecondaryIndexOp::Remove : SecondaryIndexOp::Add);
      log_set.emplace_back(std::move(snapshot));
    }
  }

  // Phase 3.3: unlock by writing the new TID. Carry the epoch forward when
  // the captured TID is from an earlier epoch.
  const EpochNumber current_epoch = epoch_framework.GetMyThreadLocalEpoch();
  std::unordered_map<DataItem*, TransactionId> unlocked_tids;
  unlocked_tids.reserve(lock_items.size());
  for (auto* item : lock_items) {
    TransactionId current = item->transaction_id.load();
    TransactionId unlocked;
    if (current.epoch == current_epoch) {
      unlocked = {current_epoch, current.tid + 1};
    } else {
      unlocked = {current_epoch, 2};
    }
    item->transaction_id.store(unlocked);
    unlocked_tids.emplace(item, unlocked);
  }

  // The log snapshot was captured under the lock and still carries the
  // locked TID; recovery would install it verbatim, and every later access
  // to the key would spin on a lock nobody owns. Publish the unlocked TID
  // into the snapshot, as the native commit path does.
  if (has_log_set) {
    for (auto& snapshot : log_set) {
      const auto tid_it = unlocked_tids.find(snapshot.index_cache);
      if (tid_it == unlocked_tids.end()) continue;
      snapshot.data_item_copy.transaction_id.store(tid_it->second);
    }
  }

  // Phase 3.4: register slots left empty by this transaction for deferred
  // physical purge, keyed by the published unlocked TID; immediate removal
  // could free memory still visible to concurrent readers.
  for (const auto& write : resolved_writes) {
    if (!write.is_delete) continue;
    auto tid_it = unlocked_tids.find(write.item);
    if (tid_it == unlocked_tids.end()) continue;
    reaper.Enqueue(write.index, nullptr, write.key, write.item,
                          tid_it->second);
  }

  std::unordered_set<DataItem*> registered_si_purges;
  for (const auto& op : resolved_si_ops) {
    if (!op.is_delete) continue;
    auto empty_it = si_empty_after_install.find(op.item);
    if (empty_it == si_empty_after_install.end() || !empty_it->second) {
      continue;
    }
    if (!registered_si_purges.insert(op.item).second) continue;
    auto tid_it = unlocked_tids.find(op.item);
    if (tid_it == unlocked_tids.end()) continue;
    reaper.Enqueue(nullptr, op.index, op.secondary_key, op.item,
                          tid_it->second);
  }

  // Phase 3.5: enqueue the log set, then leave the epoch.
  if (has_log_set) {
    [[maybe_unused]] const bool logged = logger.Enqueue(log_set, current_epoch);
  }

  epoch_framework.MakeMeOffline();
  return true;
}

}  // namespace Stateless
}  // namespace LineairDB
