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
#include <iterator>
#include <shared_mutex>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <xmmintrin.h>

#include "callback/callback_manager.h"
#include "recovery/checkpoint_manager.hpp"
#include "concurrency_control/stable_read.hpp"
#include "index/reaper.h"
#include "recovery/logger.h"
#include "stateless/commit.h"
#include "stateless/packed_transaction_id.hpp"
#include "stateless/read.h"
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

 private:
  static const Config& ValidateDataItemLayoutConfig(const Config& config) {
#ifndef LINEAIRDB_WITH_2PL_CHECKPOINT_METADATA
    // The slim DataItem layout keeps only shared dummy storage for these paths.
    if (config.enable_checkpointing) {
      SPDLOG_ERROR(
          "Unsupported configuration: checkpointing requires the full DataItem "
          "layout. Rebuild with -DLINEAIRDB_WITH_2PL_CHECKPOINT_METADATA.");
      exit(EXIT_FAILURE);
    }
    if (config.concurrency_control_protocol ==
        Config::ConcurrencyControl::TwoPhaseLocking) {
      SPDLOG_ERROR(
          "Unsupported configuration: TwoPhaseLocking requires the full "
          "DataItem layout. Rebuild with "
          "-DLINEAIRDB_WITH_2PL_CHECKPOINT_METADATA.");
      exit(EXIT_FAILURE);
    }
#endif
#ifndef LINEAIRDB_WITH_NWR
    if (config.concurrency_control_protocol ==
        Config::ConcurrencyControl::SiloNWR) {
      SPDLOG_ERROR(
          "Unsupported configuration: SiloNWR requires per-record NWR pivot "
          "metadata. Rebuild with -DLINEAIRDB_WITH_NWR.");
      exit(EXIT_FAILURE);
    }
#endif
    return config;
  }

 public:
  Impl(const Config& c = Config())
      : config_(ValidateDataItemLayoutConfig(c)),
        thread_pool_(config_.max_thread),
        logger_(config_),
        callback_manager_(config_),
        epoch_framework_(config_.epoch_duration_ms, EventsOnEpochIsUpdated()),
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
      reaper_.Reap(old_epoch);
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

  Pax::PaxStore* GetPaxStore(const std::string_view table_name) {
    auto table = GetTable(table_name);
    if (!table.has_value()) return nullptr;
    return table.value()->GetPaxStore();
  }

  bool InstallPaxSchema(const std::string_view table_name,
                        const std::vector<uint32_t>& field_max_bytes) {
    if (field_max_bytes.empty()) return false;
    // PAX blank-item routing is implemented for the Masstree backend only;
    // other index structures silently keep the heap path.
    if (config_.index_structure != Config::IndexStructure::Masstree)
      return false;
    auto table = GetTable(table_name);
    if (!table.has_value()) return false;
    Pax::TableSchema schema;
    schema.field_max_bytes = field_max_bytes;
    return table.value()->InstallPaxSchema(std::move(schema));
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

  StatelessReadResult StatelessRead(const std::string_view table_name,
                                    const std::string_view key) {
    return Stateless::Read(table_dictionary_, schema_mutex_, table_name, key);
  }

  std::vector<StatelessReadResult> StatelessBatchRead(
      const std::vector<std::pair<std::string, std::string>>& keys) {
    return Stateless::BatchRead(table_dictionary_, schema_mutex_, keys);
  }

  StatelessRangeScanResult StatelessRangeScan(
      const std::string_view table_name, const std::string_view start_key,
      const std::string_view end_key, uint64_t row_limit, bool reverse_scan) {
    return Stateless::RangeScan(table_dictionary_, schema_mutex_, table_name,
                                start_key, end_key, row_limit, reverse_scan);
  }

  StatelessPaxRefScanResult StatelessPaxRefScan(
      const std::string_view table_name, const std::string_view start_key,
      const std::string_view end_key, uint64_t row_limit, bool reverse_scan) {
    return Stateless::PaxRefScan(table_dictionary_, schema_mutex_, table_name,
                                 start_key, end_key, row_limit, reverse_scan);
  }

  StatelessSecondaryRangeScanResult StatelessSecondaryRangeScan(
      const std::string_view table_name, const std::string_view index_name,
      const std::string_view start_key, const std::string_view end_key,
      uint64_t row_limit, bool reverse_scan) {
    return Stateless::SecondaryRangeScan(table_dictionary_, schema_mutex_,
                                         table_name, index_name, start_key,
                                         end_key, row_limit, reverse_scan);
  }

  /**
   * @brief Compute exact NDV for each integer key-part prefix of one index.
   *
   * @details The proxy uses this to set MySQL `rec_per_key`. The scan counts
   * live index entries only. If any live key cannot be split as Helios integer
   * key parts, the method returns false so the proxy keeps its old estimate.
   */
  bool ComputeIndexNdvInt(const std::string_view table_name,
                          const std::string_view index_name, uint32_t num_parts,
                          std::vector<uint64_t>& out_ndv) {
    out_ndv.assign(num_parts, 0);
    if (num_parts == 0) return false;

    std::shared_lock<std::shared_mutex> lk(schema_mutex_);
    auto table = GetTable(table_name);
    if (!table.has_value()) return false;

    bool ok = true;
    bool first = true;
    // Index scans are key-ordered, so one previous key is enough for NDV.
    std::string prev_key;
    std::vector<size_t> prev_part_ends(num_parts, 0);

    // Split Helios integer key-parts: [marker][type][2-byte length][payload].
    auto count_key = [&](std::string_view key) -> bool {
      std::vector<size_t> part_ends(num_parts, 0);
      size_t offset = 0;
      for (uint32_t part = 0; part < num_parts; ++part) {
        if (offset + 4 > key.size()) {
          ok = false;
          return true;
        }
        const auto marker = static_cast<unsigned char>(key[offset]);
        const auto type = static_cast<unsigned char>(key[offset + 1]);
        if (marker != 0x00 || type != 0x10) {
          ok = false;
          return true;
        }
        const size_t len =
            (static_cast<size_t>(
                 static_cast<unsigned char>(key[offset + 2]))
             << 8) |
            static_cast<unsigned char>(key[offset + 3]);
        offset += 4 + len;
        if (offset > key.size()) {
          ok = false;
          return true;
        }
        part_ends[part] = offset;
      }

      if (first) {
        // The first live key starts one distinct prefix at every depth.
        for (uint32_t part = 0; part < num_parts; ++part) out_ndv[part] = 1;
        first = false;
      } else {
        // Count a new prefix whenever bytes up to that key-part boundary differ.
        const std::string_view prev(prev_key);
        for (uint32_t part = 0; part < num_parts; ++part) {
          if (part_ends[part] != prev_part_ends[part] ||
              key.substr(0, part_ends[part]) !=
                  prev.substr(0, prev_part_ends[part])) {
            ++out_ndv[part];
          }
        }
      }

      prev_key.assign(key.data(), key.size());
      prev_part_ends = std::move(part_ends);
      return false;
    };

    // Stable-read base liveness without copying the row payload.
    auto stable_live_base = [](const DataItem& item) {
      for (;;) {
        TransactionId tid = item.transaction_id.load();
        if (tid.tid & 1u) {
          _mm_pause();
          continue;
        }

        const bool live = item.IsPrimaryInitialized();
        if (item.transaction_id.load() == tid) return live;
      }
    };

    static const std::string kFullScanEnd(16, static_cast<char>(0xff));
    auto& primary_index = table.value()->GetPrimaryIndex();

    if (index_name.empty()) {
      // Primary index entries are base rows, so count live rows directly.
      auto scan_result = primary_index.Scan(
          std::string_view(), std::string_view(kFullScanEnd),
          [&](std::string_view key, DataItem& item) -> bool {
            if (!stable_live_base(item)) return false;
            return count_key(key);
          },
          nullptr);
      if (!scan_result.has_value()) ok = false;
    } else {
      Index::SecondaryIndex* index =
          table.value()->GetSecondaryIndex(index_name);
      if (index == nullptr) return false;

      // Pin the secondary primary-key list under one stable TID.
      auto stable_live_secondary = [&](const DataItem& item) {
        PackedPrimaryKeys::Ptr primary_keys;
        for (;;) {
          TransactionId tid = item.transaction_id.load();
          if (tid.tid & 1u) {
            _mm_pause();
            continue;
          }

          auto snapshot = std::atomic_load(&item.primary_keys_);
          const bool live = snapshot && snapshot->count != 0;
          if (item.transaction_id.load() == tid) {
            if (live) primary_keys = std::move(snapshot);
            break;
          }
        }

        // Secondary entries count only if one referenced base row is live.
        for (std::string_view primary_key :
             PackedPrimaryKeysView(primary_keys)) {
          DataItem* base_item = primary_index.Get(primary_key);
          if (base_item != nullptr && stable_live_base(*base_item)) return true;
        }
        return false;
      };

      auto scan_result = index->Scan(
          std::string_view(), std::string_view(kFullScanEnd),
          [&](std::string_view key) -> bool {
            DataItem* item = index->Get(key);
            if (item == nullptr || !stable_live_secondary(*item)) {
              return false;
            }
            return count_key(key);
          },
          nullptr);
      if (!scan_result.has_value()) ok = false;
    }

    if (!ok) {
      // Fail closed: caller keeps the old optimizer estimate.
      out_ndv.assign(num_parts, 0);
      return false;
    }
    return true;
  }

  /**
   * @brief Build an equi-depth histogram for one index's leading key part.
   *
   * @details The proxy uses the returned boundaries to estimate one-column
   * range cardinality locally. The scan is independent of NDV/rec_per_key:
   * pass 1 counts row weight, and pass 2 records the leading-key prefix at
   * each bucket boundary. Secondary-index entries are weighted by their PK
   * list size so bucket depth tracks rows, not distinct secondary keys.
   *
   * Only order-preserving fixed-layout leading parts are accepted. Unsupported
   * or malformed encodings return false, letting the proxy keep its heuristic.
   */
  bool ComputeIndexHistogram(const std::string_view table_name,
                             const std::string_view index_name, uint32_t buckets,
                             std::vector<std::string>& out_bounds,
                             std::vector<uint64_t>& out_cum) {
    out_bounds.clear();
    out_cum.clear();
    if (buckets == 0) return false;
    std::shared_lock<std::shared_mutex> lk(schema_mutex_);
    auto table = GetTable(table_name);
    if (!table.has_value()) return false;

    // Leading key-part layout: [marker][type][2-byte length][payload].
    // Accept only fixed-layout encodings whose byte order matches value order.
    auto leading_end = [](std::string_view key) -> size_t {
      if (key.size() < 4) return 0;
      if (static_cast<unsigned char>(key[0]) != 0x00) return 0;
      const unsigned char type = static_cast<unsigned char>(key[1]);
      if (type != 0x10 && type != 0x30) return 0;  // INT / DATETIME only
      const size_t len =
          (static_cast<size_t>(static_cast<unsigned char>(key[2])) << 8) |
          static_cast<unsigned char>(key[3]);
      const size_t end = 4 + len;
      return (end <= key.size()) ? end : 0;
    };

    // Stable-read base-row liveness without copying the row payload.
    const auto stable_live_base = [](DataItem& di) -> bool {
      for (;;) {
        TransactionId tid = di.transaction_id.load();
        if (tid.tid & 1u) {
          _mm_pause();
          continue;
        }
        const bool live = di.IsPrimaryInitialized();
        if (di.transaction_id.load() == tid) return live;
      }
    };

    // Secondary scans visit one entry per key, but the histogram is over rows.
    // Use the PK-list length as that key's row weight.
    const auto stable_pk_count = [](DataItem& di) -> uint64_t {
      for (;;) {
        TransactionId tid = di.transaction_id.load();
        if (tid.tid & 1u) {
          _mm_pause();
          continue;
        }
        const auto primary_keys = std::atomic_load(&di.primary_keys_);
        const uint64_t n = primary_keys ? primary_keys->count : 0;
        if (di.transaction_id.load() == tid) return n;
      }
    };
    static const std::string kMaxEnd(16, '\xff');

    // Walk one index in key order and expose each live key with its row weight.
    bool malformed = false;
    auto walk = [&](auto&& fn) {
      if (index_name.empty()) {
        table.value()->GetPrimaryIndex().Scan(
            std::string_view(), std::string_view(kMaxEnd),
            [&](std::string_view key, DataItem& di) -> bool {
              if (stable_live_base(di)) return fn(key, static_cast<uint64_t>(1));
              return false;
            },
            nullptr);
      } else {
        Index::SecondaryIndex* index =
            table.value()->GetSecondaryIndex(index_name);
        if (index == nullptr) {
          malformed = true;
          return;
        }
        index->Scan(
            std::string_view(), std::string_view(kMaxEnd),
            [&](std::string_view key) -> bool {
              DataItem* item = index->Get(key);
              if (item == nullptr) return false;
              const uint64_t w = stable_pk_count(*item);
              if (w == 0) return false;  // dead/empty secondary entry
              return fn(key, w);
            },
            nullptr);
      }
    };

    // Pass 1: count total rows represented by the index.
    uint64_t total = 0;
    walk([&](std::string_view key, uint64_t w) -> bool {
      if (leading_end(key) == 0) {
        malformed = true;
        return true;
      }
      total += w;
      return false;
    });
    if (malformed || total == 0) return false;

    // Pass 2: record a boundary at each stride-th row.
    const uint64_t stride = std::max<uint64_t>(1, total / buckets);
    uint64_t seen = 0;
    uint64_t next = stride;
    std::string last_key;
    walk([&](std::string_view key, uint64_t w) -> bool {
      const size_t end = leading_end(key);
      if (end == 0) {
        malformed = true;
        return true;
      }
      seen += w;
      last_key.assign(key.data(), end);
      if (seen >= next) {
        out_bounds.emplace_back(key.substr(0, end));
        out_cum.push_back(seen);
        while (seen >= next) next += stride;
      }
      return false;
    });
    if (malformed) {
      out_bounds.clear();
      out_cum.clear();
      return false;
    }
    if (out_bounds.empty() || out_cum.back() != total) {
      // Close the histogram at the max key so the high end is exact.
      out_bounds.push_back(last_key);
      out_cum.push_back(total);
    }
    return !out_bounds.empty();
  }

  bool ValidateAndCommit(
      const std::vector<ExternalReadEntry>& reads,
      const std::vector<ExternalWriteEntry>& writes,
      const std::vector<ExternalSecondaryIndexEntry>& secondary_index_ops,
      const std::vector<ExternalRangeReadEntry>& range_reads,
      std::string* abort_reason = nullptr) {
    return Stateless::Commit(table_dictionary_, schema_mutex_,
                             epoch_framework_, reaper_, logger_, config_,
                             reads, writes, secondary_index_ops, range_reads,
                             abort_reason);
  }

  std::optional<Table*> GetTable(const std::string_view table_name) {
    return table_dictionary_.GetTable(table_name);
  }

 private:
  void RegisterDeferredPurge(const Snapshot& snapshot,
                             TransactionId delete_commit_tid) {
    reaper_.Enqueue(snapshot, delete_commit_tid);
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
      // Skip deleted entries.
      const bool live =
          recovery_set.index_name.empty()
              ? recovery_set.data_item_copy.IsPrimaryInitialized()
              : recovery_set.data_item_copy.IsInitialized();
      if (!live) continue;
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
          SPDLOG_DEBUG(
              "  Recovery: Secondary index '{0}' restoring key '{1}' with {2} "
              "primary keys",
              recovery_set.index_name, recovery_set.key,
              recovery_set.data_item_copy.primary_keys_view().size());
          idx->Put(recovery_set.key, std::move(recovery_set.data_item_copy));
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
  Index::Reaper reaper_;
};

}  // namespace LineairDB
#endif /** LINEAIRDB_DATABASE_IMPL_H **/
