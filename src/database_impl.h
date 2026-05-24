/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#ifndef LINEAIRDB_DATABASE_IMPL_H
#define LINEAIRDB_DATABASE_IMPL_H

#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>
#include <table/table.h>

#include "index/impl/masstree_index.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "callback/callback_manager.h"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.h"
#include "table/table.h"
#include "table/table_dictionary.hpp"
#include "thread_pool/thread_pool.h"
#include "transaction_impl.h"
#include "types/snapshot.hpp"
#include "types/transaction_id.hpp"
#include "util/backoff.hpp"
#include "util/epoch_framework.hpp"
#include "util/logger.hpp"

namespace LineairDB {
class Database::Impl {
  friend class Transaction::Impl;

 public:
  inline static Database::Impl* CurrentDBInstance;

  Impl(const Config& c = Config())
      : config_(c),
        thread_pool_(c.max_thread),
        logger_(config_),
        callback_manager_(config_),
        epoch_framework_(c.epoch_duration_ms, EventsOnEpochIsUpdated()),
        checkpoint_manager_(config_, table_dictionary_, epoch_framework_) {
    // 2PL x Masstree unsupported (see 2PL ReadDirect FIXME).
    if (config_.concurrency_control_protocol ==
            Config::ConcurrencyControl::TwoPhaseLocking &&
        config_.index_structure == Config::IndexStructure::Masstree) {
      SPDLOG_ERROR(
          "Unsupported LineairDB configuration: TwoPhaseLocking + Masstree. "
          "See src/concurrency_control/impl/two_phase_locking.hpp for the "
          "supported CC x Index matrix.");
      exit(EXIT_FAILURE);
    }
    if (Database::Impl::CurrentDBInstance == nullptr) {
      Database::Impl::CurrentDBInstance = this;
      SPDLOG_INFO("LineairDB instance has been constructed.");
    } else {
      SPDLOG_ERROR(
          "It is prohibited to allocate two LineairDB::Database instance at "
          "the same time.");
      exit(EXIT_FAILURE);
    }
    if (!config_.anonymous_table_name.empty()) {
      CreateTable(config_.anonymous_table_name);
    } else {
      SPDLOG_ERROR("Anonymous table name is not set.");
      exit(EXIT_FAILURE);
    }
    if (config_.enable_recovery) {
      Recovery();
    }
    epoch_framework_.Start();
  }

  ~Impl() {
    Fence();
    thread_pool_.StopAcceptingTransactions();
    epoch_framework_.Sync();
    checkpoint_manager_.Stop();
    epoch_framework_.Stop();
    while (!thread_pool_.IsEmpty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    thread_pool_.Shutdown();
    SPDLOG_DEBUG(
        "Epoch number and Durable epoch number are ended at {0}, and {1}, "
        "respectively.",
        epoch_framework_.GetGlobalEpoch(), logger_.GetDurableEpoch());
    SPDLOG_INFO("LineairDB instance has been destructed.");
    assert(Database::Impl::CurrentDBInstance == this);
    Database::Impl::CurrentDBInstance = nullptr;
  }

  void ExecuteTransaction(ProcedureType proc, CallbackType clbk,
                          std::optional<CallbackType> prclbk) {
    for (;;) {
      bool success = thread_pool_.Enqueue([&, transaction_procedure = proc,
                                           callback = clbk,
                                           precommit_clbk = prclbk]() {
        epoch_framework_.MakeMeOnline();
        Transaction tx(this);

        transaction_procedure(tx);
        if (tx.IsAborted()) {
          if (precommit_clbk)
            precommit_clbk.value()(LineairDB::TxStatus::Aborted);
          callback(LineairDB::TxStatus::Aborted);
          epoch_framework_.MakeMeOffline();
          return;
        }

        bool committed = tx.Precommit();
        if (committed) {
          tx.tx_pimpl_->PostProcessing(TxStatus::Committed);

          if (precommit_clbk.has_value()) {
            precommit_clbk.value()(TxStatus::Committed);
          }
          const auto current_epoch = epoch_framework_.GetMyThreadLocalEpoch();
          callback_manager_.Enqueue(std::move(callback), current_epoch);
          if (config_.enable_logging) {
            logger_.Enqueue(tx.tx_pimpl_->write_set_, current_epoch);
          }
        } else {
          tx.tx_pimpl_->PostProcessing(TxStatus::Aborted);
          if (precommit_clbk.has_value()) {
            precommit_clbk.value()(TxStatus::Aborted);
          }
          callback(LineairDB::TxStatus::Aborted);
        }

        epoch_framework_.MakeMeOffline();
      });
      if (success) break;
    }
  }

  // FIXME: TLS workspace still assumes the same CC protocol across Database
  // instances on a single thread. Switching Database with a different CC
  // protocol will keep the old CC implementation.
  Transaction& BeginTransaction() {
    epoch_framework_.MakeMeOnline();
    thread_local Transaction* tls_workspace = nullptr;

    // Reuse the TLS slot only when its previous transaction has finished.
    // GetCurrentStatus() returns Running between Begin and End, so a non-Running
    // status means End has already drained the workspace.
    if (tls_workspace != nullptr &&
        tls_workspace->GetCurrentStatus() != TxStatus::Running) {
      tls_workspace->tx_pimpl_->Reset(this);
      return *tls_workspace;
    }

    auto* tx = new Transaction(this);
    if (tls_workspace == nullptr) {
      // First call on this thread: install as the per-thread workspace.
      tx->reusable_ = true;
      tls_workspace = tx;
    }
    // Otherwise the workspace is still in use by an outstanding transaction;
    // hand back a fresh one-shot Transaction that EndTransaction will delete.
    return *tx;
  }

  bool EndTransaction(Transaction& tx, CallbackType clbk) {
    if (tx.IsAborted()) {
      clbk(TxStatus::Aborted);
      if (!tx.reusable_) delete &tx;
      epoch_framework_.MakeMeOffline();
      return false;
    }

    bool committed = tx.Precommit();
    if (committed) {
      tx.tx_pimpl_->PostProcessing(TxStatus::Committed);

      tx.tx_pimpl_->current_status_ = TxStatus::Committed;
      const auto current_epoch = epoch_framework_.GetMyThreadLocalEpoch();
      callback_manager_.Enqueue(std::move(clbk), current_epoch, true);

      if (config_.enable_logging) {
        logger_.Enqueue(tx.tx_pimpl_->write_set_, current_epoch, true);
      }
    } else {
      tx.tx_pimpl_->PostProcessing(TxStatus::Aborted);
      clbk(TxStatus::Aborted);
    }
    epoch_framework_.MakeMeOffline();

    if (config_.enable_checkpointing) {
      auto checkpoint_completed =
          checkpoint_manager_.GetCheckpointCompletedEpoch();
      logger_.TruncateLogs(checkpoint_completed);
    }

    if (!tx.reusable_) delete &tx;
    return committed;
  }

  void RequestCallbacks() {
    const auto current_epoch = epoch_framework_.GetGlobalEpoch();
    callback_manager_.ExecuteCallbacks(current_epoch);
  }

  const EpochNumber& GetMyThreadLocalEpoch() {
    return epoch_framework_.GetMyThreadLocalEpoch();
  }

  /**
   * Ensures that (1) all pending operations are completed, (2) all callbacks
   * have been executed, and (3) all index updates have been fully applied and
   * are visible to subsequent operations.
   *
   * Note: Due to the dependency on the implementation of
   * moodycamel::concurrentqueue, callbacks are executed **after**
   * try_dequeue(). This means the queue size can become zero even if some
   * callbacks have not yet been executed. As a result, the current
   * `WaitForAllCallbacksToBeExecuted()` does not strictly behave as its name
   * suggests (since it only checks if the queue length is zero).
   *
   * To address this problem, an atomic variable `latest_callbacked_epoch_` is
   * used as a workaround to ensure proper waiting.
   */
  void Fence() {
    const auto current_epoch = epoch_framework_.GetGlobalEpoch();
    epoch_framework_.Sync();
    thread_pool_.WaitForQueuesToBecomeEmpty();
    callback_manager_.WaitForAllCallbacksToBeExecuted();
    {
      std::unique_lock<std::mutex> lk(fence_mtx_);
      fence_cv_.wait(lk, [&] {
        return latest_callbacked_epoch_.load() >= current_epoch;
      });
    }
    // Wait for all index updates to be linearizable
    // This ensures that all insertions/deletions are visible in the index
    table_dictionary_.ForEachTable(
        [](Table& table) { table.WaitForIndexIsLinearizable(); });
  }
  const Config& GetConfig() const { return config_; }

  // NOTE: Called by a special thread managed by EpochFramework.
  std::function<void(EpochNumber)> EventsOnEpochIsUpdated() {
    return [&](EpochNumber old_epoch) {
      // Logging
      if (config_.enable_logging) {
        EpochNumber durable_epoch = logger_.FlushDurableEpoch();
        thread_pool_.EnqueueForAllThreads(
            [&, old_epoch]() { logger_.FlushLogs(old_epoch); });
        thread_pool_.EnqueueForAllThreads([&, durable_epoch] {
          callback_manager_.ExecuteCallbacks(durable_epoch);
        });
      }

      // Execute Callbacks
      thread_pool_.EnqueueForAllThreads([&, old_epoch]() {
        callback_manager_.ExecuteCallbacks(old_epoch);
        {
          std::lock_guard<std::mutex> lk(fence_mtx_);
          latest_callbacked_epoch_.store(old_epoch);
        }
        fence_cv_.notify_all();
      });

      // Tick masstree's globalepoch so RCU can free retired leaves and
      // DataItem* limbo once min_active_epoch() catches up. Workers
      // release their epoch at tx/RPC boundaries via
      // ReleaseMasstreeThreadEpoch; we only move the watermark here.
      Index::MasstreeAdvanceEpoch();

      if (config_.enable_checkpointing) {
        auto checkpoint_completed =
            checkpoint_manager_.GetCheckpointCompletedEpoch();

        thread_pool_.EnqueueForAllThreads([&, checkpoint_completed]() {
          logger_.TruncateLogs(checkpoint_completed);
        });
      }
    };
  }

  void WaitForCheckpoint() {
    const auto start = checkpoint_manager_.GetCheckpointCompletedEpoch();
    Util::RetryWithExponentialBackoff([&]() {
      const auto current = checkpoint_manager_.GetCheckpointCompletedEpoch();
      return start != current;
    });
  }

  bool IsNeedToCheckpointing(const EpochNumber epoch) {
    return checkpoint_manager_.IsNeedToCheckpointing(epoch);
  }

  bool CreateTable(const std::string_view table_name) {
    return table_dictionary_.CreateTable(table_name, epoch_framework_, config_);
  }

  bool CreateSecondaryIndex(const std::string_view table_name,
                            const std::string_view index_name,
                            const uint index_type) {
    std::shared_lock<std::shared_mutex> lk(schema_mutex_);
    auto it = GetTable(table_name);
    if (!it.has_value()) {
      return false;
    }
    return it.value()->CreateSecondaryIndex(
        index_name,
        Index::SecondaryIndexType::FromRaw(
            static_cast<Index::SecondaryIndexType::RawType>(index_type)));
  }

  /**
   * @brief Read one row without opening a Transaction.
   *
   * Takes a shared lock on the schema, resolves the primary-index slot, and
   * performs a Silo-style double TID read on the DataItem: load the TID,
   * yield while the lock bit (LSB) is set, copy the value, then re-load the
   * TID and only return it if it has not moved. The caller keeps the
   * returned `tid` and submits it through ValidateAndCommit later.
   */
  StatelessReadResult StatelessRead(const std::string_view table_name,
                                    const std::string_view key) {
    std::shared_lock<std::shared_mutex> lk(schema_mutex_);
    auto table = GetTable(table_name);
    if (!table.has_value()) return {};

    DataItem* item = table.value()->GetPrimaryIndex().Get(key);
    if (item == nullptr) return {};

    for (;;) {
      TransactionId tid = item->transaction_id.load();
      if (tid.tid & 1u) {
        std::this_thread::yield();
        continue;
      }

      const bool found = item->IsInitialized() && item->size() != 0;
      std::string value;
      if (found) {
        value.assign(reinterpret_cast<const char*>(item->value()),
                     item->size());
      }

      if (item->transaction_id.load() == tid) {
        return {found, std::move(value), PackTransactionId(tid)};
      }
    }
  }

  /**
   * @brief Read several rows in one call.
   *
   * Reuses StatelessRead per entry. Reads are independent, so this is purely
   * a transport optimization that lets a caller fold N point reads into one
   * RPC.
   */
  std::vector<StatelessReadResult> StatelessBatchRead(
      const std::vector<std::pair<std::string, std::string>>& keys) {
    std::vector<StatelessReadResult> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
      results.emplace_back(StatelessRead(key.first, key.second));
    }
    return results;
  }

  /**
   * @brief Range-scan the primary index and return rows plus validation
   *        tokens.
   *
   * Drives Index::Scan / Index::ScanReverse with a callback that, for each
   * hit, performs the same double-TID read used by StatelessRead. The
   * Masstree scan also fills a `NodeVersionEntry` vector covering every
   * touched leaf; those are returned as `range_versions` so ValidateAndCommit
   * can run `ValidatePhantoms` later. Tombstones encountered during the scan
   * are recorded as exact-key entries in `index_reads`, because a node-version
   * check alone misses a tombstone slot being reused without a tree-shape
   * change.
   *
   * `ok` distinguishes a genuine empty result from a Masstree retry that
   * gave up. Callers should treat `!ok` as an abort signal.
   */
  StatelessRangeScanResult StatelessRangeScan(
      const std::string_view table_name, const std::string_view start_key,
      const std::string_view end_key, uint64_t row_limit, bool reverse_scan) {
    StatelessRangeScanResult result;
    if (end_key.empty()) return result;

    std::shared_lock<std::shared_mutex> lk(schema_mutex_);
    auto table = GetTable(table_name);
    if (!table.has_value()) return result;
    result.ok = true;

    // Commit re-walks the range and compares key lists, so we do not
    // ship masstree node pointers that RCU can free between RPCs.
    const bool use_logical_validation = true;
    std::vector<Index::NodeVersionEntry> versions;
    uint64_t returned_rows = 0;

    auto append_scan_entry = [&](std::string_view key, DataItem&) {
      DataItem* item = table.value()->GetPrimaryIndex().Get(key);
      if (item == nullptr) {
        return false;
      }

      for (;;) {
        TransactionId tid = item->transaction_id.load();
        if (tid.tid & 1u) {
          std::this_thread::yield();
          continue;
        }

        const bool found = item->IsInitialized() && item->size() != 0;
        std::string value;
        if (found) {
          value.assign(reinterpret_cast<const char*>(item->value()),
                       item->size());
        }

        if (item->transaction_id.load() != tid) continue;

        if (found) {
          result.rows.push_back(
              {std::string(key), std::move(value), PackTransactionId(tid),
               true});
          ++returned_rows;
        }
        // Tombstones are skipped: Purge erases them at commit, and key-list
        // validation catches any reuse without needing a per-entry TID.
        return row_limit > 0 && returned_rows >= row_limit;
      }
    };

    auto scan_result =
        reverse_scan
            ? table.value()->GetPrimaryIndex().ScanReverse(
                  start_key, end_key, append_scan_entry,
                  use_logical_validation ? nullptr : &versions)
            : table.value()->GetPrimaryIndex().Scan(
                  start_key, end_key, append_scan_entry,
                  use_logical_validation ? nullptr : &versions);
    if (!scan_result.has_value()) {
      result.rows.clear();
      result.range_versions.clear();
      result.index_reads.clear();
      return result;
    }

    if (use_logical_validation) {
      ExternalRangeValidationEntry logical_range;
      logical_range.table_name = std::string(table_name);
      logical_range.start_key = std::string(start_key);
      logical_range.end_key = std::string(end_key);
      logical_range.row_limit = row_limit;
      logical_range.reverse_scan = reverse_scan;
      logical_range.result_keys.reserve(result.rows.size());
      for (const auto& row : result.rows) {
        logical_range.result_keys.push_back(row.key);
      }
      result.range_versions.push_back(std::move(logical_range));
    } else {
      result.range_versions.reserve(versions.size());
      for (const auto& version : versions) {
        ExternalRangeValidationEntry entry;
        entry.table_name = std::string(table_name);
        entry.owner_ptr = reinterpret_cast<uint64_t>(version.owner);
        entry.node_ptr = reinterpret_cast<uint64_t>(version.node_ptr);
        entry.version = version.version;
        result.range_versions.push_back(std::move(entry));
      }
    }
    return result;
  }

  /**
   * @brief Range-scan a secondary index and resolve each hit to its base
   *        row.
   *
   * For every secondary key in `[start_key, end_key)`, looks up its
   * `primary_keys()` and, for each one, performs the same double-TID base
   * read as StatelessRead. Each secondary slot's TID is also captured as an
   * `ExternalIndexValidationEntry` so SI rewrites at commit time are caught.
   * Like StatelessRangeScan, the Masstree-side node versions feed
   * `range_versions` for phantom validation, and `ok == false` is the abort
   * signal.
   */
  StatelessSecondaryRangeScanResult StatelessSecondaryRangeScan(
      const std::string_view table_name, const std::string_view index_name,
      const std::string_view start_key, const std::string_view end_key,
      uint64_t row_limit, bool reverse_scan) {
    StatelessSecondaryRangeScanResult result;
    if (end_key.empty()) return result;

    std::shared_lock<std::shared_mutex> lk(schema_mutex_);
    auto table = GetTable(table_name);
    if (!table.has_value()) return result;

    Index::SecondaryIndex* index =
        table.value()->GetSecondaryIndex(index_name);
    if (index == nullptr) return result;
    result.ok = true;

    // Same rationale as StatelessRangeScan above.
    const bool use_logical_validation = true;
    std::vector<Index::NodeVersionEntry> versions;
    uint64_t returned_rows = 0;

    auto snapshot_base_row = [&](const std::string& secondary_key,
                                 const std::string& primary_key) {
      DataItem* item = table.value()->GetPrimaryIndex().Get(primary_key);
      if (item == nullptr) {
        return false;
      }

      for (;;) {
        TransactionId tid = item->transaction_id.load();
        if (tid.tid & 1u) {
          std::this_thread::yield();
          continue;
        }

        const bool found = item->IsInitialized() && item->size() != 0;
        std::string value;
        if (found) {
          value.assign(reinterpret_cast<const char*>(item->value()),
                       item->size());
        }

        if (item->transaction_id.load() != tid) continue;

        if (found) {
          result.rows.push_back({secondary_key, primary_key, std::move(value),
                                 PackTransactionId(tid), true});
          ++returned_rows;
        }
        return row_limit > 0 && returned_rows >= row_limit;
      }
    };

    auto append_secondary_entry = [&](std::string_view key) {
      const std::string secondary_key(key);
      DataItem* item = index->Get(key);
      if (item == nullptr) {
        if (!use_logical_validation) {
          result.index_reads.push_back(
              {std::string(table_name), std::string(index_name),
               secondary_key, 0, false});
        }
        return false;
      }

      std::vector<std::string> primary_keys;
      for (;;) {
        TransactionId tid = item->transaction_id.load();
        if (tid.tid & 1u) {
          std::this_thread::yield();
          continue;
        }

        const bool found = item->IsInitialized() &&
                           !item->primary_keys().empty();
        if (found) primary_keys = item->primary_keys();

        if (item->transaction_id.load() != tid) continue;

        if (!use_logical_validation) {
          result.index_reads.push_back(
              {std::string(table_name), std::string(index_name),
               secondary_key, PackTransactionId(tid), found});
        }
        break;
      }

      for (const auto& primary_key : primary_keys) {
        if (snapshot_base_row(secondary_key, primary_key)) return true;
      }
      return false;
    };

    auto scan_result =
        reverse_scan ? index->ScanReverse(start_key, end_key,
                                          append_secondary_entry,
                                          use_logical_validation ? nullptr
                                                                 : &versions)
                     : index->Scan(start_key, end_key,
                                   append_secondary_entry,
                                   use_logical_validation ? nullptr
                                                          : &versions);
    if (!scan_result.has_value()) {
      result.rows.clear();
      result.range_versions.clear();
      result.index_reads.clear();
      return result;
    }

    if (use_logical_validation) {
      ExternalRangeValidationEntry logical_range;
      logical_range.table_name = std::string(table_name);
      logical_range.index_name = std::string(index_name);
      logical_range.start_key = std::string(start_key);
      logical_range.end_key = std::string(end_key);
      logical_range.row_limit = row_limit;
      logical_range.reverse_scan = reverse_scan;
      logical_range.result_keys.reserve(result.rows.size());
      logical_range.result_primary_keys.reserve(result.rows.size());
      for (const auto& row : result.rows) {
        logical_range.result_keys.push_back(row.secondary_key);
        logical_range.result_primary_keys.push_back(row.primary_key);
      }
      result.range_versions.push_back(std::move(logical_range));
    } else {
      result.range_versions.reserve(versions.size());
      for (const auto& version : versions) {
        ExternalRangeValidationEntry entry;
        entry.table_name = std::string(table_name);
        entry.index_name = std::string(index_name);
        entry.owner_ptr = reinterpret_cast<uint64_t>(version.owner);
        entry.node_ptr = reinterpret_cast<uint64_t>(version.node_ptr);
        entry.version = version.version;
        result.range_versions.push_back(std::move(entry));
      }
    }
    return result;
  }

  /**
   * @brief Run the Silo commit phase against caller-supplied snapshots.
   *
   * Steps, in order:
   *   1. Epoch join
   *        call `MakeMeOnline` so the thread participates in this epoch.
   *   2. Resolve
   *        under `schema_mutex_` shared, map each read, write, and
   *        secondary-index op to its DataItem. Writes use `GetOrInsert`;
   *        when that splits a Masstree leaf for our own insert,
   *        `reconcile_own_insert` advances the matching `range_reads` entry
   *        from `old_version` to `new_version` so the transaction does not
   *        self-abort.
   *   3. Pre-lock UNIQUE dedup
   *        the same UNIQUE SI key appearing twice in `secondary_index_ops`
   *        is rejected with `unique_si_duplicate_in_request`.
   *   4. Lock
   *        address-sort the write targets and CAS the lock bit (LSB) into
   *        each TID. Remember the `before_lock` TID per item.
   *   5. Epoch fence
   *        bounce off and back on so a fresh epoch starts.
   *   6. Exact-read validation
   *        every ExternalReadEntry and ExternalIndexValidationEntry has its
   *        DataItem's TID compared with the supplied `tid`. For items we
   *        just locked, the comparison uses `before_lock`. Mismatches abort
   *        with `exact_read_tid_moved` (or `index_read_tid_moved`).
   *   7. Range validation
   *        for each `range_reads` entry, either replay the scan and compare
   *        key lists (logical form), or call `ValidatePhantoms` on the
   *        recorded Masstree node version (physical form). Failure aborts
   *        with `range_node_version_changed` or `*_range_result_changed`.
   *   8. Post-lock UNIQUE recheck
   *        simulate the SI slot state across all ops on the same item to
   *        catch a concurrent insert that became visible while we were
   *        waiting on the write lock. Failure aborts with
   *        `unique_si_exists_after_lock`.
   *   9. Write-target recheck
   *        re-`Get` each write target's index entry; abort with
   *        `write_target_replaced` or `si_target_replaced` if a concurrent
   *        committed delete purged the slot between Step 2 and the lock.
   *  10. Op dedup
   *        collapse same-key writes/SI ops with last-wins so Step 11 does
   *        not Purge a key and then install into the orphaned slot.
   *  11. Install
   *        Purge then `DataItem::Reset` for deletes, then apply SI
   *        add/remove via `AddSecondaryIndexValue` /
   *        `RemoveSecondaryIndexValue`. SI entries are Purged when their
   *        post-tx primary_keys list is empty.
   *  12. Log snapshot
   *        capture the post-install snapshot before unlock so a later
   *        transaction cannot overwrite the values we just logged. Only
   *        when logging is enabled.
   *  13. Unlock
   *        rewrite each item's TID. Carry the epoch forward when the
   *        captured TID is from an earlier epoch.
   *  14. Log enqueue
   *        push the log set into `logger_`, then call `MakeMeOffline`.
   *
   * On any failure path the function unlocks every item it owns, sets
   * `*abort_reason` (when non-null) to a short label such as
   * `exact_read_tid_moved`, `range_node_version_changed`,
   * `unique_si_exists_after_lock`, or `write_target_replaced`, and
   * returns false.
   */
  bool ValidateAndCommit(
      const std::vector<ExternalReadEntry>& reads,
      const std::vector<ExternalWriteEntry>& writes,
      const std::vector<ExternalSecondaryIndexEntry>& secondary_index_ops,
      const std::vector<ExternalRangeValidationEntry>& range_reads,
      const std::vector<ExternalIndexValidationEntry>& index_reads,
      std::string* abort_reason = nullptr) {
    // Step 1: epoch join.
    epoch_framework_.MakeMeOnline();
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

    std::vector<ValidationEntry> validation_entries;
    std::vector<ResolvedWrite> resolved_writes;
    std::vector<ResolvedSecondaryIndexOp> resolved_si_ops;
    std::vector<DataItem*> lock_items;
    std::unordered_set<std::string> unique_si_adds;
    auto reconciled_range_reads = range_reads;

    auto reconcile_own_insert = [&](const Index::NodeVersionUpdate& update) {
      if (!update.valid || update.node_ptr == nullptr) return true;
      for (auto& entry : reconciled_range_reads) {
        if (!entry.end_key.empty()) continue;
        if (entry.owner_ptr != reinterpret_cast<uint64_t>(update.owner) ||
            entry.node_ptr != reinterpret_cast<uint64_t>(update.node_ptr)) {
          continue;
        }
        if (entry.version == update.old_version) {
          entry.version = update.new_version;
          continue;
        }
        return false;
      }
      return true;
    };

    auto abort_before_lock = [&](const std::string& reason) {
      if (abort_reason != nullptr) *abort_reason = reason;
      epoch_framework_.MakeMeOffline();
      return false;
    };

    // Step 2: resolve reads, writes, and SI ops to their DataItems.
    {
      std::shared_lock<std::shared_mutex> lk(schema_mutex_);

      // Resolve point reads to the DataItem and version observed by proxy
      validation_entries.reserve(reads.size() + index_reads.size());
      for (const auto& read : reads) {
        auto table = GetTable(read.table_name);
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

      // Resolve exact index entries read by stateless scans.
      // Empty index_name means a primary-index entry such as a tombstone.
      for (const auto& read : index_reads) {
        auto table = GetTable(read.table_name);
        if (!table.has_value()) {
          if (read.found || read.tid != 0) {
            return abort_before_lock("index_read_table_missing");
          }
          continue;
        }

        if (read.index_name.empty()) {
          DataItem* item = table.value()->GetPrimaryIndex().Get(read.key);
          validation_entries.push_back({table.value(), nullptr,
                                        read.table_name, "", read.key, item,
                                        UnpackTransactionId(read.tid),
                                        read.found});
        } else {
          Index::SecondaryIndex* index =
              table.value()->GetSecondaryIndex(read.index_name);
          if (index == nullptr) {
            if (read.found || read.tid != 0) {
              return abort_before_lock("index_read_index_missing");
            }
            continue;
          }

          DataItem* item = index->Get(read.key);
          validation_entries.push_back({table.value(), index, read.table_name,
                                        read.index_name, read.key, item,
                                        UnpackTransactionId(read.tid),
                                        read.found});
        }
      }

      // Resolve row writes and deletes to the primary-index entries to lock
      resolved_writes.reserve(writes.size());
      for (const auto& write : writes) {
        auto table = GetTable(write.table_name);
        if (!table.has_value()) {
          return abort_before_lock("write_table_missing");
        }

        Index::NodeVersionUpdate own_insert;
        DataItem* item =
            table.value()->GetPrimaryIndex().GetOrInsert(write.key,
                                                         &own_insert);
        if (item == nullptr) {
          return abort_before_lock("write_get_or_insert_failed");
        }
        if (!reconcile_own_insert(own_insert)) {
          return abort_before_lock("range_node_version_changed");
        }

        resolved_writes.push_back({write.table_name, write.key, write.value,
                                   write.is_delete, item});
        lock_items.push_back(item);
      }

      // Resolve secondary-index updates to the secondary-index entries to lock
      resolved_si_ops.reserve(secondary_index_ops.size());
      for (const auto& op : secondary_index_ops) {
        auto table = GetTable(op.table_name);
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

        Index::NodeVersionUpdate own_insert;
        DataItem* item = nullptr;
        if (op.is_delete) {
          item = index->GetOrInsert(op.secondary_key, &own_insert);
        } else {
          item = index->GetOrInsertForWrite(op.secondary_key, &own_insert);
        }
        if (item == nullptr) {
          return abort_before_lock("si_get_or_insert_failed");
        }
        if (!reconcile_own_insert(own_insert)) {
          return abort_before_lock("range_node_version_changed");
        }

        resolved_si_ops.push_back({op.table_name, op.index_name,
                                   op.secondary_key, op.primary_key,
                                   op.is_delete, item, index,
                                   index->GetIndexType()});
        lock_items.push_back(item);
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
      for (auto* item : lock_items) {
        TransactionId current = item->transaction_id.load();
        if (current.tid & 1u) {
          current.tid--;
          item->transaction_id.store(current);
        }
      }
      epoch_framework_.MakeMeOffline();
      return false;
    };

    // Lock loop: spin until the LSB CAS lands.
    for (auto* item : lock_items) {
      for (;;) {
        TransactionId current = item->transaction_id.load();
        if (current.tid & 1u) {
          std::this_thread::yield();
          continue;
        }
        TransactionId locked = current;
        locked.tid |= 1u;
        if (item->transaction_id.compare_exchange_weak(current, locked)) {
          locked_tids.push_back({item, current, locked});
          break;
        }
      }
    }

    // Step 5: epoch fence so a fresh epoch covers the validation phase.
    epoch_framework_.MakeMeOffline();
    epoch_framework_.MakeMeOnline();

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
      DataItem* item = read.item;
      if (item == nullptr) {
        item = read.index == nullptr
                   ? read.table->GetPrimaryIndex().Get(read.key)
                   : read.index->Get(read.key);
        if (item == nullptr) continue;
        if (!item->IsInitialized() && !read.found) continue;
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

    // Step 7: range validation (logical key list and physical node version).
    auto is_own_locked = [&](DataItem* item) {
      for (const auto& locked : locked_tids) {
        if (locked.item == item) return true;
      }
      return false;
    };

    auto wait_until_readable = [&](DataItem* item) {
      for (;;) {
        TransactionId tid = item->transaction_id.load();
        if (!(tid.tid & 1u) || is_own_locked(item)) return;
        std::this_thread::yield();
      }
    };

    auto validate_primary_key_list =
        [&](const ExternalRangeValidationEntry& range) {
          auto table = GetTable(range.table_name);
          if (!table.has_value()) return false;

          std::vector<std::string> keys;
          auto collect_key = [&](std::string_view key, DataItem& item) {
            wait_until_readable(&item);
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
          return scan_result.has_value() && keys == range.result_keys;
        };

    auto validate_secondary_key_list =
        [&](const ExternalRangeValidationEntry& range) {
          auto table = GetTable(range.table_name);
          if (!table.has_value()) return false;
          auto* index = table.value()->GetSecondaryIndex(range.index_name);
          if (index == nullptr) return false;

          std::vector<std::string> secondary_keys;
          std::vector<std::string> primary_keys;
          auto snapshot_base_row = [&](const std::string& secondary_key,
                                       const std::string& primary_key) {
            DataItem* item = table.value()->GetPrimaryIndex().Get(primary_key);
            if (item == nullptr) return false;
            wait_until_readable(item);
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
            wait_until_readable(item);
            if (!item->IsInitialized() || item->primary_keys().empty()) {
              return false;
            }
            for (const auto& primary_key : item->primary_keys()) {
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
          return scan_result.has_value() &&
                 secondary_keys == range.result_keys &&
                 primary_keys == range.result_primary_keys;
        };

    for (const auto& range : reconciled_range_reads) {
      if (range.end_key.empty()) continue;
      const bool ok = range.index_name.empty()
                          ? validate_primary_key_list(range)
                          : validate_secondary_key_list(range);
      if (!ok) {
        return unlock_and_abort(range.index_name.empty()
                                    ? "primary_range_result_changed"
                                    : "secondary_range_result_changed");
      }
    }

    // Step 7 (physical form): re-check Masstree node versions captured by
    // earlier stateless scans.
    std::vector<Index::NodeVersionEntry> range_versions;
    std::unordered_set<Index::IndexBase*> range_owners;
    range_versions.reserve(reconciled_range_reads.size());
    for (const auto& range : reconciled_range_reads) {
      if (!range.end_key.empty()) continue;
      if (range.owner_ptr == 0 || range.node_ptr == 0) {
        return unlock_and_abort("range_node_token_missing");
      }
      auto* owner = reinterpret_cast<Index::IndexBase*>(range.owner_ptr);
      range_owners.insert(owner);
      range_versions.push_back(
          {owner, reinterpret_cast<const void*>(range.node_ptr),
           range.version});
    }
    for (auto* owner : range_owners) {
      if (!owner->ValidatePhantoms(range_versions)) {
        return unlock_and_abort("range_node_version_changed");
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

    // Step 9: re-check that each write target's index entry still
    // points to our locked DataItem. A concurrent committed delete may
    // have purged the slot between Step 2 resolution and the lock, in
    // which case installing here would write to an orphaned pointer.
    // SI ops are checked too: an SI slot is purged once its primary_keys
    // vector empties.
    for (const auto& write : resolved_writes) {
      auto table = GetTable(write.table_name);
      if (!table.has_value()) continue;
      if (table.value()->GetPrimaryIndex().Get(write.key) != write.item) {
        return unlock_and_abort("write_target_replaced");
      }
    }
    for (const auto& op : resolved_si_ops) {
      if (op.index == nullptr) continue;
      if (op.index->Get(op.secondary_key) != op.item) {
        return unlock_and_abort("si_target_replaced");
      }
    }

    // Step 10: coalesce duplicate ops by key (last-wins). The proxy
    // ships ops in arrival order; running [DELETE k, WRITE k] as-is
    // would Purge k and then Reset() into the orphaned slot, losing the
    // insert. SI has the analogous race on repeated (sk, pk).
    {
      std::unordered_map<std::string, size_t> last_idx;
      for (size_t i = 0; i < resolved_writes.size(); ++i) {
        const auto& w = resolved_writes[i];
        last_idx[w.table_name + '\0' + w.key] = i;
      }
      std::vector<ResolvedWrite> deduped;
      deduped.reserve(last_idx.size());
      for (size_t i = 0; i < resolved_writes.size(); ++i) {
        const auto& w = resolved_writes[i];
        const std::string key = w.table_name + '\0' + w.key;
        if (last_idx[key] == i) {
          deduped.push_back(std::move(resolved_writes[i]));
        }
      }
      resolved_writes = std::move(deduped);
    }
    {
      std::unordered_map<std::string, size_t> last_idx;
      for (size_t i = 0; i < resolved_si_ops.size(); ++i) {
        const auto& op = resolved_si_ops[i];
        last_idx[op.table_name + '\0' + op.index_name + '\0' +
                 op.secondary_key + '\0' + op.primary_key] = i;
      }
      std::vector<ResolvedSecondaryIndexOp> deduped;
      deduped.reserve(last_idx.size());
      for (size_t i = 0; i < resolved_si_ops.size(); ++i) {
        const auto& op = resolved_si_ops[i];
        const std::string key = op.table_name + '\0' + op.index_name + '\0' +
                                op.secondary_key + '\0' + op.primary_key;
        if (last_idx[key] == i) {
          deduped.push_back(std::move(resolved_si_ops[i]));
        }
      }
      resolved_si_ops = std::move(deduped);
    }

    // Step 11: install row writes/deletes and SI add/remove. Purge
    // BEFORE Reset on deletes: MasstreeIndex::Insert treats an
    // uninitialized DataItem as a reusable slot, so flipping first would
    // let a racing Insert grab it and our erase would drop the new value.
    for (auto& write : resolved_writes) {
      if (write.is_delete) {
        auto table = GetTable(write.table_name);
        if (table.has_value()) {
          table.value()->GetPrimaryIndex().Purge(write.key, write.item);
        }
        write.item->Reset(nullptr, 0);
      } else {
        write.item->Reset(reinterpret_cast<const std::byte*>(write.value.data()),
                          write.value.size());
      }
    }

    // Install secondary-index add/remove. The Purge decision uses the
    // post-tx primary_keys, not the intermediate per-op state — for a
    // unique SI [Remove(sk,pk1), Add(sk,pk2)] (= UPDATE on a unique
    // column), a per-op decision would Purge after Remove and then
    // Add(pk2) into the orphaned slot, losing the mapping. Purge again
    // runs BEFORE mutating DataItem (same ordering as the primary loop).
    std::unordered_map<DataItem*, std::vector<std::string>> si_final_state;
    for (const auto& op : resolved_si_ops) {
      auto [it, inserted] = si_final_state.emplace(op.item,
                                                   std::vector<std::string>{});
      if (inserted) {
        it->second = op.item->primary_keys();
      }
      auto& keys = it->second;
      if (op.is_delete) {
        keys.erase(std::remove(keys.begin(), keys.end(), op.primary_key),
                   keys.end());
      } else {
        if (std::find(keys.begin(), keys.end(), op.primary_key) == keys.end()) {
          keys.push_back(op.primary_key);
        }
      }
    }
    // Purge each item whose post-tx primary_keys is empty, before any
    // DataItem mutation. Any op on the same item gives the (sk, index)
    // pair we need.
    std::unordered_set<DataItem*> physical_deleted;
    for (const auto& op : resolved_si_ops) {
      if (physical_deleted.count(op.item)) continue;
      auto it = si_final_state.find(op.item);
      if (it == si_final_state.end() || !it->second.empty()) continue;
      op.index->Purge(op.secondary_key, op.item);
      physical_deleted.insert(op.item);
    }
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

    // Step 12: build the log snapshot before unlock so a later transaction
    // cannot overwrite the values we just logged.
    WriteSetType log_set;
    bool has_log_set = false;
    if (config_.enable_logging) {
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

    // Step 13: unlock by writing the new TID. Carry the epoch forward when
    // the captured TID is from an earlier epoch.
    const EpochNumber current_epoch = epoch_framework_.GetMyThreadLocalEpoch();
    for (auto* item : lock_items) {
      TransactionId current = item->transaction_id.load();
      TransactionId unlocked;
      if (current.epoch == current_epoch) {
        unlocked = {current_epoch, current.tid + 1};
      } else {
        unlocked = {current_epoch, 2};
      }
      item->transaction_id.store(unlocked);
    }

    // Step 14: enqueue the log set, then leave the epoch.
    if (has_log_set) {
      logger_.Enqueue(log_set, current_epoch, true);
    }

    epoch_framework_.MakeMeOffline();
    return true;
  }

  std::optional<Table*> GetTable(const std::string_view table_name) {
    return table_dictionary_.GetTable(table_name);
  }

 private:
  /// Pack a {epoch, tid} pair into one uint64_t so it can travel over the
  /// stateless RPC as an opaque version token.
  static uint64_t PackTransactionId(const TransactionId& tid) {
    return (static_cast<uint64_t>(tid.epoch) << 32) |
           static_cast<uint64_t>(tid.tid);
  }

  /// Inverse of PackTransactionId. The stateless API hands the packed value
  /// back through ValidateAndCommit, which unpacks it to compare with the
  /// live TransactionId on the DataItem.
  static TransactionId UnpackTransactionId(uint64_t packed) {
    return {static_cast<EpochNumber>(packed >> 32),
            static_cast<uint32_t>(packed & 0xffffffffu)};
  }

  void Recovery() {
    SPDLOG_INFO("Start recovery process");
    // Start recovery from logfiles
    EpochNumber highest_epoch = 1;
    const auto durable_epoch = logger_.GetDurableEpochFromLog();
    SPDLOG_DEBUG("  Durable epoch is resumed from {0}", highest_epoch);
    logger_.SetDurableEpoch(durable_epoch);
    [[maybe_unused]] auto enqueued = thread_pool_.EnqueueForAllThreads(
        [&]() { logger_.RememberMe(durable_epoch); });
    assert(enqueued);

    thread_pool_.WaitForQueuesToBecomeEmpty();

    epoch_framework_.MakeMeOnline();

    auto& local_epoch = epoch_framework_.GetMyThreadLocalEpoch();
    local_epoch = durable_epoch;

    highest_epoch = std::max(highest_epoch, durable_epoch);
    auto&& recovery_sets = logger_.GetRecoverySetFromLogs(durable_epoch);

    for (auto& recovery_set : recovery_sets) {
      // Skip deleted entries (tombstones with size=0)
      if (!recovery_set.data_item_copy.IsInitialized()) continue;
      CreateTable(recovery_set.table_name);
      auto table = GetTable(recovery_set.table_name);
      if (!table.has_value()) {
        SPDLOG_CRITICAL(
            "Recovery failed: Table {0} could not be found or created.",
            recovery_set.table_name);
        exit(EXIT_FAILURE);
      }

      highest_epoch =
          std::max(highest_epoch,
                   recovery_set.data_item_copy.transaction_id.load().epoch);

      if (recovery_set.index_name.empty()) {
        // Primary Index recovery
        table.value()->GetPrimaryIndex().Put(
            recovery_set.key, std::move(recovery_set.data_item_copy));
      } else {
        // Secondary Index recovery
        Index::SecondaryIndex* idx = nullptr;
        table.value()->GetOrCreateSecondaryIndex(recovery_set.index_name,
                                                 recovery_set.index_type, &idx);
        if (idx != nullptr) {
          idx->Put(recovery_set.key, std::move(recovery_set.data_item_copy));
          SPDLOG_DEBUG(
              "  Recovery: Secondary index '{0}' restored key '{1}' with {2} "
              "primary keys",
              recovery_set.index_name, recovery_set.key,
              recovery_set.data_item_copy.primary_keys().size());
        } else {
          SPDLOG_ERROR(
              "Recovery failed: Could not create secondary index {0} for "
              "table {1}",
              recovery_set.index_name, recovery_set.table_name);
        }
      }
    }
    epoch_framework_.MakeMeOffline();

    SPDLOG_DEBUG("  Global epoch is resumed from {0}", highest_epoch);
    epoch_framework_.SetGlobalEpoch(highest_epoch);
    SPDLOG_INFO("Finish recovery process");
  }

 private:
  Config config_;
  ThreadPool thread_pool_;
  Recovery::Logger logger_;
  Callback::CallbackManager callback_manager_;
  EpochFramework epoch_framework_;
  TableDictionary table_dictionary_;
  std::atomic<EpochNumber> latest_callbacked_epoch_{1};
  std::mutex fence_mtx_;
  std::condition_variable fence_cv_;
  Recovery::CPRManager checkpoint_manager_;
  mutable std::shared_mutex schema_mutex_;
};

}  // namespace LineairDB
#endif /** LINEAIRDB_DATABASE_IMPL_H **/
