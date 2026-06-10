#include "stateless/commit.h"

#include <algorithm>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "index/concurrent_table.h"
#include "index/reaper.h"
#include "index/secondary_index.h"
#include "recovery/logger.h"
#include "stateless/packed_transaction_id.hpp"
#include "table/table.h"
#include "table/table_dictionary.hpp"
#include "types/data_item.hpp"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Stateless {

/**
 * @brief Run the Silo commit phase against caller-supplied snapshots.
 *
 * Steps, in order:
 *   1. Epoch join
 *        call `MakeMeOnline` so the thread participates in this epoch.
 *   2. Resolve
 *        under `schema_mutex` shared, map each read, write, and
 *        secondary-index op to its DataItem. Writes use `GetOrInsert`.
 *   3. Pre-lock UNIQUE dedup
 *        the same UNIQUE SI key appearing twice in `secondary_index_ops`
 *        is rejected with `unique_si_duplicate_in_request`.
 *   4. Lock
 *        address-sort the write targets and CAS the lock bit (LSB) into
 *        each TID. Remember the `before_lock` TID per item.
 *   5. Epoch fence
 *        bounce off and back on so a fresh epoch starts.
 *   6. Exact-read validation
 *        every ExternalReadEntry has its DataItem's TID compared with the
 *        supplied `tid`. For items we just locked, the comparison uses
 *        `before_lock`. Mismatches abort with `exact_read_tid_moved`.
 *   7. Range validation
 *        for each `range_reads` entry, replay the scan and compare key
 *        lists. Failure aborts with `*_range_result_changed`.
 *   8. Post-lock UNIQUE recheck
 *        simulate the SI slot state across all ops on the same item to
 *        catch a concurrent insert that became visible while we were
 *        waiting on the write lock. Failure aborts with
 *        `unique_si_exists_after_lock`.
 *   9. Install
 *        `DataItem::Reset` for deletes, then apply SI add/remove via
 *        `AddSecondaryIndexValue` / `RemoveSecondaryIndexValue`. Empty
 *        primary and SI slots stay in the tree as tombstones.
 *  10. Log snapshot
 *        capture the post-install snapshot before unlock so a later
 *        transaction cannot overwrite the values we just logged. Only
 *        when logging is enabled.
 *  11. Unlock
 *        rewrite each item's TID. Carry the epoch forward when the
 *        captured TID is from an earlier epoch, then register tombstones
 *        for later physical purge using the published unlocked TID.
 *  12. Log enqueue
 *        push the log set into `logger`, then call `MakeMeOffline`.
 *
 * Range validation is logical-only: a stateless caller cannot hold a
 * Masstree node pointer across the RPC boundary, so a physical
 * node-version token (a `range_reads` entry with an empty `end_key`) or a
 * non-empty `index_reads` set is rejected up front with
 * `physical_range_token_unsupported` / `index_reads_unsupported` instead
 * of being silently skipped.
 *
 * On any failure path the function unlocks every item it owns, sets
 * `*abort_reason` (when non-null) to a short label such as
 * `exact_read_tid_moved`, `primary_range_result_changed`, or
 * `unique_si_exists_after_lock`, and returns false.
 */
bool Commit(TableDictionary& tables, std::shared_mutex& schema_mutex,
            EpochFramework& epoch_framework, Index::Reaper& reaper,
            Recovery::Logger& logger, const Config& config,
            const std::vector<ExternalReadEntry>& reads,
            const std::vector<ExternalWriteEntry>& writes,
            const std::vector<ExternalSecondaryIndexEntry>& secondary_index_ops,
            const std::vector<ExternalRangeValidationEntry>& range_reads,
            const std::vector<ExternalIndexValidationEntry>& index_reads,
            std::string* abort_reason) {
  // Step 1: epoch join.
  epoch_framework.MakeMeOnline();
  if (abort_reason != nullptr) abort_reason->clear();

  struct ValidationEntry {
    Table* table = nullptr;
    Index::SecondaryIndex* index = nullptr;
    std::string table_name;
    std::string index_name;
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

  // Validation is logical-only. A range entry with an empty end_key is a
  // physical Masstree node-version token and exact index entries have no
  // producer; reject both instead of silently skipping them.
  if (!index_reads.empty()) {
    return abort_before_lock("index_reads_unsupported");
  }
  for (const auto& range : range_reads) {
    if (range.end_key.empty()) {
      return abort_before_lock("physical_range_token_unsupported");
    }
  }

  // Step 2: resolve reads, writes, and SI ops to their DataItems.
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
      validation_entries.push_back({table.value(), nullptr,
                                    read.table_name, "", read.key, item,
                                    UnpackTransactionId(read.tid),
                                    read.found});
    }

    // Resolve row writes and deletes to the primary-index entries to lock
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

      // Step 3: reject the same UNIQUE SI key appearing twice in this request.
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

  // Step 4: address-sort and CAS-lock every write target.
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

  // Step 5: epoch fence so a fresh epoch covers the validation phase.
  epoch_framework.MakeMeOffline();
  epoch_framework.MakeMeOnline();

  // Step 6: re-read exact-key TIDs and confirm they have not moved.
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
    if (!read.index_name.empty()) {
      out += ':';
      out += read.index_name;
    }
    out += ":key=";
    out += key_hex(read.key);
    return out;
  };

  for (const auto& read : validation_entries) {
    DataItem* item = read.index == nullptr
                         ? read.table->GetPrimaryIndex().Get(read.key)
                         : read.index->Get(read.key);
    if (item == nullptr) {
      // A read observed as present must still resolve at validation
      // time. An unresolvable key here means a committed delete purged
      // the slot after the read, which is a serializability conflict.
      if (read.found) {
        return unlock_and_abort(exact_read_reason(
            read.index_name.empty() ? "exact_read_disappeared"
                                    : "index_read_disappeared",
            read));
      }
      continue;
    }

    if (!read.found) {
      if (!item->IsInitialized()) {
        continue;
      }
      return unlock_and_abort(exact_read_reason(
          read.index_name.empty() ? "exact_read_appeared"
                                  : "index_read_appeared",
          read));
    }

    TransactionId expected = read.captured_tid;
    for (const auto& locked : locked_tids) {
      if (locked.item == item) {
        if (locked.before_lock.epoch != read.captured_tid.epoch ||
            locked.before_lock.tid != read.captured_tid.tid) {
          return unlock_and_abort(exact_read_reason(
              read.index_name.empty() ? "exact_read_tid_moved"
                                      : "index_read_tid_moved",
              read));
        }
        expected = locked.locked;
        break;
      }
    }

    if (item->transaction_id.load() != expected) {
      return unlock_and_abort(exact_read_reason(
          read.index_name.empty() ? "exact_read_tid_moved"
                                  : "index_read_tid_moved",
          read));
    }
    if (read.found && !item->IsInitialized()) {
      return unlock_and_abort(exact_read_reason(
          read.index_name.empty() ? "exact_read_deleted"
                                  : "index_read_deleted",
          read));
    }
  }

  // Step 7: replay each range scan and compare the key lists.
  auto is_own_locked = [&](DataItem* item) {
    for (const auto& locked : locked_tids) {
      if (locked.item == item) return true;
    }
    return false;
  };

  // Silo Phase 2 read validation is wait-free: a record locked by another
  // transaction is treated as "dirty" and forces abort. Spinning here breaks
  // the paper's deadlock-freedom invariant — sorted write-lock acquisition
  // protects only write-to-write edges, not read-to-write edges introduced
  // by validators.
  auto try_snapshot_readable = [&](DataItem* item) -> bool {
    TransactionId tid = item->transaction_id.load();
    if (!(tid.tid & 1u)) return true;
    return is_own_locked(item);
  };

  auto validate_primary_key_list =
      [&](const ExternalRangeValidationEntry& range) {
        auto table = tables.GetTable(range.table_name);
        if (!table.has_value()) return false;

        std::vector<std::string> keys;
        bool aborted = false;
        auto collect_key = [&](std::string_view key, DataItem& item) {
          if (!try_snapshot_readable(&item)) {
            aborted = true;
            return true;
          }
          if (item.IsInitialized() && item.size() != 0) {
            keys.emplace_back(key);
          }
          return range.row_limit > 0 && keys.size() >= range.row_limit;
        };

        auto scan_result =
            range.reverse_scan
                ? table.value()->GetPrimaryIndex().ScanReverse(
                      range.start_key, range.end_key, collect_key, nullptr)
                : table.value()->GetPrimaryIndex().Scan(
                      range.start_key, range.end_key, collect_key, nullptr);
        if (aborted) return false;
        return scan_result.has_value() && keys == range.result_keys;
      };

  auto validate_secondary_key_list =
      [&](const ExternalRangeValidationEntry& range) {
        auto table = tables.GetTable(range.table_name);
        if (!table.has_value()) return false;
        auto* index = table.value()->GetSecondaryIndex(range.index_name);
        if (index == nullptr) return false;

        std::vector<std::string> secondary_keys;
        std::vector<std::string> primary_keys;
        bool aborted = false;
        auto snapshot_base_row = [&](const std::string& secondary_key,
                                     const std::string& primary_key) {
          DataItem* item = table.value()->GetPrimaryIndex().Get(primary_key);
          if (item == nullptr) return false;
          if (!try_snapshot_readable(item)) {
            aborted = true;
            return true;
          }
          if (item->IsInitialized() && item->size() != 0) {
            secondary_keys.push_back(secondary_key);
            primary_keys.push_back(primary_key);
          }
          return range.row_limit > 0 &&
                 primary_keys.size() >= range.row_limit;
        };

        auto collect_secondary_key = [&](std::string_view key) {
          const std::string secondary_key(key);
          DataItem* item = index->Get(key);
          if (item == nullptr) return false;
          // Copy the primary-key list under a double-TID read, as in the
          // staging scan: a committer mutates the live vector in place
          // under its lock. The TID stays constant while locked, so the
          // lock bit is checked before the copy and the TID after it.
          const TransactionId observed = item->transaction_id.load();
          if ((observed.tid & 1u) && !is_own_locked(item)) {
            aborted = true;
            return true;
          }
          const bool initialized = item->IsInitialized();
          std::vector<std::string> item_primary_keys;
          if (initialized) item_primary_keys = item->primary_keys();
          if (item->transaction_id.load() != observed) {
            aborted = true;
            return true;
          }
          if (!initialized || item_primary_keys.empty()) {
            return false;
          }
          for (const auto& primary_key : item_primary_keys) {
            if (snapshot_base_row(secondary_key, primary_key)) return true;
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
        return scan_result.has_value() &&
               secondary_keys == range.result_keys &&
               primary_keys == range.result_primary_keys;
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

  // Step 8: post-lock UNIQUE recheck. A competing add may have installed
  // the same secondary key while we were waiting on the write lock, so the
  // pre-lock dedup at step 3 is not enough on its own.
  std::unordered_map<DataItem*, std::vector<std::string>> si_primary_keys;
  for (const auto& op : resolved_si_ops) {
    if (!op.index_type.IsUnique()) continue;

    auto [state_it, inserted] =
        si_primary_keys.emplace(op.item, std::vector<std::string>{});
    if (inserted) state_it->second = op.item->primary_keys();
    auto& keys = state_it->second;

    auto key_it = std::lower_bound(keys.begin(), keys.end(),
                                   op.primary_key);
    const bool key_exists =
        key_it != keys.end() && *key_it == op.primary_key;

    if (op.is_delete) {
      if (key_exists) keys.erase(key_it);
      continue;
    }

    if (!keys.empty()) {
      return unlock_and_abort("unique_si_exists_after_lock");
    }
    keys.insert(key_it, op.primary_key);
  }

  // Step 9: install row writes/deletes and SI add/remove. Deletes leave
  // tombstones in the tree; physical removal is deferred until a later
  // epoch so same-key reinserts reuse the slot and advance its TID chain.
  for (auto& write : resolved_writes) {
    if (write.is_delete) {
      write.item->Reset(nullptr, 0);
    } else {
      write.item->Reset(reinterpret_cast<const std::byte*>(write.value.data()),
                        write.value.size());
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
  // primary_keys vector must not be read after unlock: a concurrent
  // committer mutates it in place or swaps its buffer under its own lock.
  // Computed after the whole install loop so a delete-then-add sequence on
  // the same slot within this transaction reads the final state.
  std::unordered_map<DataItem*, bool> si_empty_after_install;
  for (const auto& op : resolved_si_ops) {
    if (!op.is_delete) continue;
    si_empty_after_install[op.item] = op.item->primary_keys().empty();
  }

  // Step 10: build the log snapshot before unlock so a later transaction
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

  // Step 11: unlock by writing the new TID. Carry the epoch forward when
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

  // Step 12: enqueue the log set, then leave the epoch.
  if (has_log_set) {
    logger.Enqueue(log_set, current_epoch, true);
  }

  epoch_framework.MakeMeOffline();
  return true;
}

}  // namespace Stateless
}  // namespace LineairDB
