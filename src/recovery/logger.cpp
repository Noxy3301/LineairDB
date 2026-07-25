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

#include "logger.h"

#include <lineairdb/config.h>

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <unordered_map>
#include <utility>
#include <util/logger.hpp>

#include "impl/thread_local_logger.h"
#include "types/definitions.h"

namespace LineairDB {
namespace Recovery {

namespace {

/**
 * Folds decoded records into the write set the database replays.
 *
 * A key may appear in several epochs; the newest transaction id wins. Secondary
 * index entries arrive as per-primary-key deltas and are regrouped into one
 * entry per secondary key, so a key deleted after being added does not come
 * back.
 */
WriteSetType BuildRecoverySet(const LogRecords& records) {
  struct SecondaryOpKey {
    std::string table_name;
    std::string index_name;
    uint32_t index_type;
    std::string secondary_key;
    std::string primary_key;
    bool operator==(const SecondaryOpKey& rhs) const {
      return table_name == rhs.table_name && index_name == rhs.index_name &&
             index_type == rhs.index_type &&
             secondary_key == rhs.secondary_key &&
             primary_key == rhs.primary_key;
    }
  };
  struct SecondaryOpKeyHash {
    size_t operator()(const SecondaryOpKey& key) const {
      const std::hash<std::string> hasher;
      size_t seed = hasher(key.table_name);
      seed ^= hasher(key.index_name) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= std::hash<uint32_t>{}(key.index_type) + 0x9e3779b9 + (seed << 6) +
              (seed >> 2);
      seed ^=
          hasher(key.secondary_key) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= hasher(key.primary_key) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      return seed;
    }
  };
  struct SecondaryOpState {
    TransactionId tid;
    SecondaryIndexOp op;
  };
  struct SecondaryGroupKey {
    std::string table_name;
    std::string index_name;
    uint32_t index_type;
    std::string secondary_key;
    bool operator==(const SecondaryGroupKey& rhs) const {
      return table_name == rhs.table_name && index_name == rhs.index_name &&
             index_type == rhs.index_type && secondary_key == rhs.secondary_key;
    }
  };
  struct SecondaryGroupKeyHash {
    size_t operator()(const SecondaryGroupKey& key) const {
      const std::hash<std::string> hasher;
      size_t seed = hasher(key.table_name);
      seed ^= hasher(key.index_name) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      seed ^= std::hash<uint32_t>{}(key.index_type) + 0x9e3779b9 + (seed << 6) +
              (seed >> 2);
      seed ^=
          hasher(key.secondary_key) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      return seed;
    }
  };
  struct SecondaryGroupValue {
    TransactionId max_tid{};
    std::vector<std::string> primary_keys;
  };

  std::unordered_map<SecondaryOpKey, SecondaryOpState, SecondaryOpKeyHash>
      secondary_latest;
  WriteSetType recovery_set;

  for (const auto& log_record : records) {
    for (const auto& kvp : log_record.key_value_pairs) {
      const auto op = static_cast<SecondaryIndexOp>(kvp.secondary_op);
      const bool is_secondary_index =
          !kvp.index_name.empty() || op != SecondaryIndexOp::None ||
          !kvp.primary_keys.empty() || !kvp.secondary_primary_key.empty() ||
          kvp.index_type != 0;
      if (is_secondary_index) {
        if (op == SecondaryIndexOp::Full) {
          for (const auto& pk : kvp.primary_keys) {
            SecondaryOpKey op_key{kvp.table_name, kvp.index_name,
                                  kvp.index_type, kvp.key, pk};
            auto it = secondary_latest.find(op_key);
            if (it == secondary_latest.end() || it->second.tid < kvp.tid) {
              secondary_latest[op_key] = {kvp.tid, SecondaryIndexOp::Add};
            }
          }
        } else if (!kvp.secondary_primary_key.empty()) {
          SecondaryOpKey op_key{kvp.table_name, kvp.index_name, kvp.index_type,
                                kvp.key, kvp.secondary_primary_key};
          auto it = secondary_latest.find(op_key);
          if (it == secondary_latest.end() || it->second.tid < kvp.tid) {
            secondary_latest[op_key] = {kvp.tid, op};
          }
        }
        continue;
      }

      const std::byte* value_ptr =
          kvp.buffer.empty()
              ? nullptr
              : reinterpret_cast<const std::byte*>(kvp.buffer.data());
      bool not_found = true;
      for (auto& item : recovery_set) {
        if (item.key == kvp.key && item.table_name == kvp.table_name &&
            item.index_name == kvp.index_name) {
          not_found = false;
          if (item.data_item_copy.transaction_id.load() < kvp.tid) {
            item.data_item_copy.Reset(value_ptr, kvp.buffer.size(), kvp.tid);
            item.table_name = kvp.table_name;
            item.index_name = kvp.index_name;
            item.index_type =
                Index::SecondaryIndexType::FromRaw(kvp.index_type);
          }
        }
      }
      if (not_found) {
        Snapshot snapshot = {
            kvp.key,
            reinterpret_cast<const std::byte*>(kvp.buffer.data()),
            kvp.buffer.size(),
            nullptr,
            kvp.table_name,
            kvp.index_name,
            kvp.tid,
            Index::SecondaryIndexType::FromRaw(kvp.index_type),
        };
        recovery_set.emplace_back(std::move(snapshot));
      }
    }
  }

  std::unordered_map<SecondaryGroupKey, SecondaryGroupValue,
                     SecondaryGroupKeyHash>
      grouped_secondary;
  for (const auto& [op_key, state] : secondary_latest) {
    if (state.op != SecondaryIndexOp::Add) continue;
    SecondaryGroupKey group_key{op_key.table_name, op_key.index_name,
                                op_key.index_type, op_key.secondary_key};
    auto& entry = grouped_secondary[group_key];
    entry.primary_keys.emplace_back(op_key.primary_key);
    if (entry.max_tid < state.tid) entry.max_tid = state.tid;
  }

  for (auto& [group_key, entry] : grouped_secondary) {
    if (entry.primary_keys.empty()) continue;
    std::sort(entry.primary_keys.begin(), entry.primary_keys.end());
    entry.primary_keys.erase(
        std::unique(entry.primary_keys.begin(), entry.primary_keys.end()),
        entry.primary_keys.end());
    Snapshot snapshot = {group_key.secondary_key,
                         nullptr,
                         0,
                         nullptr,
                         group_key.table_name,
                         group_key.index_name,
                         entry.max_tid,
                         Index::SecondaryIndexType::FromRaw(
                             group_key.index_type)};
    snapshot.data_item_copy.SetPrimaryKeys(std::move(entry.primary_keys));
    snapshot.data_item_copy.Reset(nullptr, 0, entry.max_tid);
    recovery_set.emplace_back(std::move(snapshot));
  }
  return recovery_set;
}

}  // namespace

Logger::Logger(const Config& config, WalIo io) : work_dir_(config.work_dir) {
  LineairDB::Util::SetUpSPDLog();
  logger_ = std::make_unique<ThreadLocalLogger>(
      config, [this](EpochNumber frontier) { PublishDurable(frontier); },
      [this](int error_number) { PublishFailure(error_number); },
      [this]() { return GetDurableEpoch(); }, std::move(io));
}

Logger::~Logger() {
  StopAndDrainFlusher();
  logger_.reset();
}

bool Logger::Enqueue(const WriteSetType& ws_ref, EpochNumber epoch) {
  return logger_->Enqueue(ws_ref, epoch);
}

Logger::RecoveryResult Logger::Recover() {
  auto scan = logger_->ScanAndRepairWal();
  RecoveryResult result;
  if (scan.status != WalScanResult::Status::Ok) {
    SPDLOG_CRITICAL("Durability Error: {0} ({1}), errno {2}", scan.detail,
                    scan.status == WalScanResult::Status::Corrupt ? "corrupt"
                                                                 : "I/O error",
                    scan.error_number);
    PublishFailure(scan.error_number != 0 ? scan.error_number : EIO);
    result.status = RecoveryStatus::Failed;
    return result;
  }

  durable_epoch_.store(scan.frontier, std::memory_order_seq_cst);
  result.frontier = scan.frontier;
  result.recovery_set = BuildRecoverySet(scan.records);
  return result;
}

void Logger::StartFlusher() { logger_->StartFlusher(); }

void Logger::ScheduleFlush(EpochNumber closed) {
  logger_->ScheduleFlush(closed);
}

bool Logger::IsQuiescent() { return logger_->IsQuiescent(); }

void Logger::StopAndDrainFlusher() {
  if (logger_) logger_->StopAndDrainFlusher();
  PublishStopped();
}

void Logger::PublishDurable(EpochNumber frontier) {
  {
    std::lock_guard<std::mutex> lock(durability_mutex_);
    const EpochNumber previous = durable_epoch_.load(std::memory_order_seq_cst);
    if (frontier < previous) {
      // The frontier is the promise the commit path hands to clients; moving it
      // backwards would retract an acknowledgement.
      SPDLOG_CRITICAL(
          "Durability Error: the durable epoch moved backwards, {0} to {1}",
          previous, frontier);
      std::abort();
    }
    if (state_ != State::Running) return;
    durable_epoch_.store(frontier, std::memory_order_seq_cst);
  }
  durability_cv_.notify_all();
}

void Logger::PublishFailure(int error_number) {
  {
    std::lock_guard<std::mutex> lock(durability_mutex_);
    if (state_ == State::Failed) return;
    state_ = State::Failed;
    failure_errno_ = error_number;
    SPDLOG_CRITICAL(
        "Durability Error: the log cannot be written (errno {0}); no further "
        "commit is acknowledged as durable",
        error_number);
  }
  durability_cv_.notify_all();
}

void Logger::PublishStopped() {
  {
    std::lock_guard<std::mutex> lock(durability_mutex_);
    if (state_ == State::Running) state_ = State::Stopped;
  }
  durability_cv_.notify_all();
}

Logger::WaitResult Logger::WaitUntilDurable(EpochNumber commit_epoch,
                                           Deadline deadline) {
  if (durable_epoch_.load(std::memory_order_seq_cst) >= commit_epoch) {
    return WaitResult::Durable;
  }

  std::unique_lock<std::mutex> lock(durability_mutex_);
  const auto reached = [&] {
    return durable_epoch_.load(std::memory_order_seq_cst) >= commit_epoch ||
           state_ != State::Running;
  };
  if (deadline == Deadline::max()) {
    durability_cv_.wait(lock, reached);
  } else if (!durability_cv_.wait_until(lock, deadline, reached)) {
    return WaitResult::TimedOut;
  }

  // A frontier that already covers this epoch outranks a terminal state: the
  // records are on the device regardless of what happened afterwards.
  if (durable_epoch_.load(std::memory_order_seq_cst) >= commit_epoch) {
    return WaitResult::Durable;
  }
  return state_ == State::Stopped ? WaitResult::Stopped : WaitResult::Failed;
}

}  // namespace Recovery
}  // namespace LineairDB
