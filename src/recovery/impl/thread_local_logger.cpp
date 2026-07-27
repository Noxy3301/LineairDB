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
#include "util/debug_sync.hpp"

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
  assert(!preparer_.joinable());
  assert(!flusher_.joinable());
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    prepared_through_ = read_durable_();
  }
  flusher_ = std::thread([this]() { FlusherLoop(); });
  preparer_ = std::thread([this]() { PreparerLoop(); });
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
  // The preparer and the I/O stage share this condition variable. A closed
  // target is work only for the preparer, so notify_one could wake the I/O
  // waiter, have it reject the predicate, and leave the preparer asleep.
  work_cv_.notify_all();
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
  if (preparer_.joinable()) preparer_.join();
  if (flusher_.joinable()) flusher_.join();
}

void ThreadLocalLogger::Fail(int error_number) {
  bool first_failure = false;
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (!failed_) {
      failed_       = true;
      first_failure = true;
    }
  }
  work_cv_.notify_all();
  if (first_failure) publish_failure_(error_number);
}

void ThreadLocalLogger::PreparerLoop() {
  for (;;) {
    EpochNumber target = 0;
    EpochNumber prepared_before = 0;
    {
      std::unique_lock<std::mutex> lock(state_mutex_);
      work_cv_.wait(lock, [this] {
        return failed_ ||
               (!prepared_.has_value() &&
                pending_closed_ > prepared_through_) ||
               (stop_requested_ && pending_closed_ <= prepared_through_);
      });
      if (failed_) return;
      if (stop_requested_ && pending_closed_ <= prepared_through_) {
        preparer_done_ = true;
        lock.unlock();
        work_cv_.notify_all();
        return;
      }
      assert(!prepared_.has_value());
      target          = pending_closed_;
      prepared_before = prepared_through_;
    }

    PreparedFlush prepared;
    WalAppendResult result;
    try {
      result = PrepareThrough(target, prepared_before, &prepared);
    } catch (const std::exception& e) {
      SPDLOG_CRITICAL("Durability Error: the WAL preparer threw: {0}", e.what());
      result = {false, EIO};
    } catch (...) {
      SPDLOG_CRITICAL("Durability Error: the WAL preparer threw");
      result = {false, EIO};
    }

    if (!result.ok) {
      Fail(result.error_number);
      return;
    }

    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      if (failed_) return;
      assert(!prepared_.has_value());
      prepared_         = std::move(prepared);
      prepared_through_ = target;
    }
    work_cv_.notify_all();
  }
}

void ThreadLocalLogger::FlusherLoop() {
  for (;;) {
    PreparedFlush prepared;
    {
      std::unique_lock<std::mutex> lock(state_mutex_);
      work_cv_.wait(lock, [this] {
        return failed_ || prepared_.has_value() || preparer_done_;
      });
      if (failed_) return;
      if (!prepared_.has_value()) {
        assert(preparer_done_);
        return;
      }
      prepared = std::move(*prepared_);
      prepared_.reset();
    }
    // The queue slot is free before I/O starts. This is the overlap: the
    // preparer may now collect and encode the next closed target while this
    // thread remains blocked in fdatasync.
    work_cv_.notify_all();

    WalAppendResult result;
    try {
      result = wal_.AppendEncodedGroup(&prepared.encoded);
    } catch (const std::exception& e) {
      SPDLOG_CRITICAL("Durability Error: the WAL I/O stage threw: {0}", e.what());
      result = {false, EIO};
    } catch (...) {
      SPDLOG_CRITICAL("Durability Error: the WAL I/O stage threw");
      result = {false, EIO};
    }
    if (!result.ok) {
      Fail(result.error_number);
      return;
    }

    auto& trace = FlushTrace::Instance();
    if (trace.Enabled()) {
      prepared.trace.write_begin = prepared.encoded.write_begin;
      prepared.trace.write_end   = prepared.encoded.write_end;
      prepared.trace.sync_begin  = prepared.encoded.sync_begin;
      prepared.trace.sync_end    = prepared.encoded.sync_end;
    }
    const bool traced           = trace.Enabled();
    const int64_t publish_enter = traced ? FlushTrace::Now() : 0;
    publish_durable_(prepared.target);
    if (traced) {
      trace.GroupPublish(std::move(prepared.trace), prepared.target,
                         publish_enter, FlushTrace::Now());
    }
  }
}

WalAppendResult ThreadLocalLogger::PrepareThrough(
    EpochNumber target, EpochNumber prepared_before,
    PreparedFlush* prepared) {
  assert(prepared != nullptr);
  const EpochNumber durable_before = read_durable_();
  auto& trace                       = FlushTrace::Instance();
  prepared->target                  = target;
  prepared->trace                   = trace.GroupBegin(durable_before);

  nodes_.ForEach([&](ThreadLocalStorageNode* node) {
    LogRecords swapped;
    {
      std::lock_guard<std::mutex> lock(node->log_records_mutex);
      swapped.swap(node->log_records);
    }
    for (auto& record : swapped) {
      if (record.epoch <= prepared_before) {
        // A producer publishes OFFLINE only after Enqueue returns, so an epoch
        // already handed to the I/O stage cannot gain a record afterwards. In
        // the pipelined design that target may not be durable yet, which is why
        // this check uses the prepared frontier rather than the published one.
        SPDLOG_CRITICAL(
            "Durability Error: a record for epoch {0} arrived after {1} was "
            "prepared for WAL I/O",
            record.epoch, prepared_before);
        std::abort();
      }
      carry_[record.epoch].emplace_back(std::move(record));
    }
  });

  if (trace.Enabled()) prepared->trace.collect_end = FlushTrace::Now();

  const auto result = wal_.EncodeGroup(carry_, target, &prepared->encoded);
  if (!result.ok) return result;
  if (trace.Enabled()) {
    prepared->trace.encode_begin  = prepared->encoded.encode_begin;
    prepared->trace.encode_end    = prepared->encoded.encode_end;
    prepared->trace.encoded_bytes = prepared->encoded.bytes.size();
    prepared->trace.epoch_count   = prepared->encoded.epoch_count;
  }
  LINEAIRDB_DEBUG_SYNC("wal.after_encode");
  // Buckets above the target stay for the next group; the encoded bytes now own
  // everything removed here until the I/O stage either syncs them or fail-stops.
  carry_.erase(carry_.begin(), carry_.upper_bound(target));
  return result;
}

}  // namespace Recovery
}  // namespace LineairDB
