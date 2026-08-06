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

#include "thread_local_logger.h"

#include <errno.h>

#include <cassert>
#include <cstdlib>
#include <exception>
#include <iterator>
#include <utility>
#include <util/logger.hpp>

#include "recovery/flush_trace.h"
#include "types/definitions.h"

namespace LineairDB {
namespace Recovery {

ThreadLocalLogger::ThreadLocalLogger(const Config& config,
                                     PublishDurable publish_durable,
                                     PublishFailure publish_failure,
                                     ReadDurable read_durable, WalIo io)
    : wal_(config.work_dir, std::move(io),
           config.commit_durability == Config::CommitDurability::Volatile
               ? Wal::kNoPreallocation
               : config.wal_initial_capacity_bytes),
      publish_durable_(std::move(publish_durable)),
      publish_failure_(std::move(publish_failure)),
      read_durable_(std::move(read_durable)) {
  LineairDB::Util::SetUpSPDLog();
}

ThreadLocalLogger::~ThreadLocalLogger() { StopAndDrainFlusher(); }

bool ThreadLocalLogger::Enqueue(const WriteSetType& ws_ref, EpochNumber epoch) {
  LogRecord record;
  record.epoch = epoch;

  for (auto& snapshot : ws_ref) {
    if (snapshot.index_name.empty()) {
      LogRecord::KeyValuePair kvp;
      kvp.key = snapshot.key;
      kvp.buffer = snapshot.data_item_copy.buffer.toString();
      kvp.tid = snapshot.data_item_copy.transaction_id.load();
      kvp.table_name = snapshot.table_name;
      kvp.index_name = snapshot.index_name;
      kvp.index_type = snapshot.index_type.Raw();
      kvp.primary_keys = snapshot.data_item_copy.primary_keys_vector();
      kvp.secondary_op = static_cast<uint8_t>(SecondaryIndexOp::None);
      record.key_value_pairs.emplace_back(std::move(kvp));
      continue;
    }

    if (snapshot.secondary_index_deltas.empty()) continue;
    for (const auto& delta : snapshot.secondary_index_deltas) {
      LogRecord::KeyValuePair kvp;
      kvp.key = snapshot.key;
      kvp.buffer = snapshot.data_item_copy.buffer.toString();
      kvp.tid = snapshot.data_item_copy.transaction_id.load();
      kvp.table_name = snapshot.table_name;
      kvp.index_name = snapshot.index_name;
      kvp.index_type = snapshot.index_type.Raw();
      kvp.secondary_op = static_cast<uint8_t>(delta.op);
      kvp.secondary_primary_key = delta.primary_key;
      record.key_value_pairs.emplace_back(std::move(kvp));
    }
  }

  // Decided after building the record, not from the input write set: a write set
  // of secondary snapshots that carry no delta produces nothing to persist, and
  // the commit path must not wait for a record that was never buffered.
  if (record.key_value_pairs.empty()) return false;

  auto* node = nodes_.Get();
  std::lock_guard<std::mutex> lock(node->log_records_mutex);
  node->log_records.emplace_back(std::move(record));
  return true;
}

WalScanResult ThreadLocalLogger::ScanAndRepairWal() {
  return wal_.ScanAndRepair();
}

void ThreadLocalLogger::StartFlusher() {
  assert(!flusher_.joinable());
  flusher_ = std::thread([this]() { FlusherLoop(); });
}

void ThreadLocalLogger::ScheduleFlush(EpochNumber closed) {
  auto& trace               = FlushTrace::Instance();
  const bool traced         = trace.Enabled();
  const int64_t close_enter = traced ? FlushTrace::Now() : 0;
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (stop_requested_ || failed_) return;
    if (closed > pending_closed_) pending_closed_ = closed;
  }
  // The hand-over is timed before the flusher is woken and recorded after, so
  // the census never sits between the state change and the notification.
  const int64_t close_exit = traced ? FlushTrace::Now() : 0;
  work_cv_.notify_one();
  if (traced) trace.EpochClosed(closed, close_enter, close_exit);
}

bool ThreadLocalLogger::IsQuiescent() {
  std::lock_guard<std::mutex> lock(state_mutex_);
  return failed_ || pending_closed_ <= read_durable_();
}

void ThreadLocalLogger::StopAndDrainFlusher() {
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    stop_requested_ = true;
  }
  work_cv_.notify_all();
  if (flusher_.joinable()) flusher_.join();
}

void ThreadLocalLogger::FlusherLoop() {
  for (;;) {
    EpochNumber target = 0;
    {
      std::unique_lock<std::mutex> lock(state_mutex_);
      work_cv_.wait(lock, [this] {
        return stop_requested_ || failed_ ||
               pending_closed_ > read_durable_();
      });
      if (failed_) return;
      target = pending_closed_;
      const bool nothing_to_do = target <= read_durable_();
      // Stop only once everything already closed is on the device, so a clean
      // shutdown does not drop records the tick had handed over.
      if (stop_requested_ && nothing_to_do) return;
      if (nothing_to_do) continue;
    }

    // Serialization and file I/O run without state_mutex_; a committing
    // thread contends only for its own node's short buffer lock, never
    // behind serialization or fdatasync.
    WalAppendResult result;
    try {
      result = FlushThrough(target);
    } catch (const std::exception& e) {
      SPDLOG_CRITICAL("Durability Error: the flusher threw: {0}", e.what());
      result = {false, EIO};
    } catch (...) {
      SPDLOG_CRITICAL("Durability Error: the flusher threw");
      result = {false, EIO};
    }

    if (!result.ok) {
      {
        std::lock_guard<std::mutex> lock(state_mutex_);
        failed_ = true;
      }
      publish_failure_(result.error_number);
      return;
    }
    // An empty eligible range publishes without an append: the skipped
    // epochs carry no record, and an epoch with nothing to persist is
    // durable by definition.
    auto& trace                 = FlushTrace::Instance();
    const bool traced           = trace.Enabled();
    const int64_t publish_enter = traced ? FlushTrace::Now() : 0;
    publish_durable_(target);
    if (traced) trace.GroupPublish(target, publish_enter, FlushTrace::Now());
  }
}

WalAppendResult ThreadLocalLogger::FlushThrough(EpochNumber target) {
  const EpochNumber durable_before = read_durable_();
  auto& trace       = FlushTrace::Instance();
  const bool traced = trace.Enabled();
  if (traced) trace.GroupCollectBegin(durable_before);

  nodes_.ForEach([&](ThreadLocalStorageNode* node) {
    LogRecords swapped;
    {
      std::lock_guard<std::mutex> lock(node->log_records_mutex);
      swapped.swap(node->log_records);
    }
    for (auto& record : swapped) {
      if (record.epoch <= durable_before) {
        // A producer publishes OFFLINE only after Enqueue returns, so an epoch
        // the writer has already closed cannot gain a record afterwards.
        // Reaching here means the closure the durability contract rests on is
        // broken, and continuing would acknowledge a record that is not on the
        // device.
        SPDLOG_CRITICAL(
            "Durability Error: a record for epoch {0} arrived after {1} was "
            "reported durable",
            record.epoch, durable_before);
        std::abort();
      }
      carry_[record.epoch].emplace_back(std::move(record));
    }
  });

  if (traced) trace.GroupCollectEnd();

  const auto result = wal_.AppendGroup(carry_, target);
  if (!result.ok) return result;
  // Buckets above the target stay for the next group; the ones just written are
  // the only ones dropped.
  carry_.erase(carry_.begin(), carry_.upper_bound(target));
  return result;
}

}  // namespace Recovery
}  // namespace LineairDB
