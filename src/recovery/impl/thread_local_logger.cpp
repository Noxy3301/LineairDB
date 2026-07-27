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

#include <algorithm>
#include <cassert>
#include <charconv>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iterator>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <util/logger.hpp>

#include "recovery/flush_trace.h"
#include "types/definitions.h"

namespace LineairDB {
namespace Recovery {

namespace {

std::string LaneFileName(size_t lane_id) {
  return lane_id == 0 ? "wal.log"
                      : "wal." + std::to_string(lane_id) + ".log";
}

/**
 * Refuses a configuration that would silently ignore an existing lane.
 *
 * Growing the lane count is safe: every old file is scanned and new lanes start
 * empty at the recovered frontier. Shrinking is not, because records in a file
 * beyond the new count would disappear from recovery.
 */
void RejectExcludedLaneFiles(const std::string& work_dir, size_t lane_count) {
  const std::filesystem::path directory(work_dir);
  if (!std::filesystem::exists(directory)) return;

  constexpr std::string_view prefix = "wal.";
  constexpr std::string_view suffix = ".log";
  for (const auto& entry : std::filesystem::directory_iterator(directory)) {
    if (!entry.is_regular_file()) continue;
    const std::string name = entry.path().filename().string();
    if (name.size() <= prefix.size() + suffix.size() ||
        name.compare(0, prefix.size(), prefix) != 0 ||
        name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) {
      continue;
    }
    const std::string_view number(
        name.data() + prefix.size(),
        name.size() - prefix.size() - suffix.size());
    size_t lane_id = 0;
    const auto [end, error] =
        std::from_chars(number.data(), number.data() + number.size(), lane_id);
    if (error != std::errc{} || end != number.data() + number.size() ||
        lane_id == 0) {
      throw std::runtime_error("unrecognised WAL lane file " +
                               entry.path().string());
    }
    if (lane_id >= lane_count) {
      throw std::runtime_error(
          "WAL lane count " + std::to_string(lane_count) +
          " would ignore existing " + entry.path().string());
    }
  }
}

}  // namespace

ThreadLocalLogger::ThreadLocalLogger(const Config& config,
                                     PublishDurable publish_durable,
                                     PublishFailure publish_failure,
                                     ReadDurable read_durable, WalIo io)
    : publish_durable_(std::move(publish_durable)),
      publish_failure_(std::move(publish_failure)) {
  if (config.wal_lane_count == 0) {
    throw std::invalid_argument("WAL lane count must be at least one");
  }
  RejectExcludedLaneFiles(config.work_dir, config.wal_lane_count);
  const uint64_t total_capacity =
      config.commit_durability == Config::CommitDurability::Volatile
          ? Wal::kNoPreallocation
          : config.wal_initial_capacity_bytes;
  const uint64_t lane_capacity =
      total_capacity == 0
          ? 0
          : total_capacity / config.wal_lane_count +
                (total_capacity % config.wal_lane_count != 0 ? 1 : 0);
  lanes_.reserve(config.wal_lane_count);
  for (size_t lane_id = 0; lane_id < config.wal_lane_count; ++lane_id) {
    lanes_.emplace_back(std::make_unique<Lane>(
        config.work_dir, io, lane_capacity, LaneFileName(lane_id)));
  }
  // Kept in the constructor signature for LoggerBase compatibility. Lane-local
  // frontiers, rather than the outer logger's minimum, govern late records.
  (void)read_durable;
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
  size_t lane_id = node->lane_id.load(std::memory_order_acquire);
  if (lane_id == kUnassignedLane) {
    const size_t proposed =
        next_lane_.fetch_add(1, std::memory_order_relaxed) % lanes_.size();
    size_t expected = kUnassignedLane;
    if (node->lane_id.compare_exchange_strong(
            expected, proposed, std::memory_order_release,
            std::memory_order_acquire)) {
      lane_id = proposed;
    } else {
      lane_id = expected;
    }
  }
  assert(lane_id < lanes_.size());
  std::lock_guard<std::mutex> lock(node->log_records_mutex);
  node->log_records.emplace_back(std::move(record));
  return true;
}

WalScanResult ThreadLocalLogger::ScanAndRepairWal() {
  WalScanResult combined;
  for (auto& lane : lanes_) {
    auto scanned = lane->wal.ScanAndRepair();
    if (scanned.status != WalScanResult::Status::Ok) return scanned;
    combined.frontier = std::max(combined.frontier, scanned.frontier);
    combined.tail_truncated =
        combined.tail_truncated || scanned.tail_truncated;
    combined.records.insert(
        combined.records.end(),
        std::make_move_iterator(scanned.records.begin()),
        std::make_move_iterator(scanned.records.end()));
  }
  std::stable_sort(combined.records.begin(), combined.records.end(),
                   [](const LogRecord& lhs, const LogRecord& rhs) {
                     return lhs.epoch < rhs.epoch;
                   });
  // Startup has now proved that every lane contains no further complete record.
  // New epochs resume above the highest recovered one, so an old lane whose last
  // actual frame was earlier can safely join at the common recovered frontier.
  for (auto& lane : lanes_) {
    lane->durable.store(combined.frontier, std::memory_order_seq_cst);
  }
  published_durable_ = combined.frontier;
  return combined;
}

void ThreadLocalLogger::StartFlusher() {
  for (size_t lane_id = 0; lane_id < lanes_.size(); ++lane_id) {
    assert(!lanes_[lane_id]->flusher.joinable());
    lanes_[lane_id]->flusher =
        std::thread([this, lane_id]() { FlusherLoop(lane_id); });
  }
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
  work_cv_.notify_all();
  if (traced) trace.EpochClosed(closed, close_enter, close_exit);
}

bool ThreadLocalLogger::IsQuiescent() {
  std::lock_guard<std::mutex> lock(state_mutex_);
  return failed_ || pending_closed_ <= MinimumDurable();
}

void ThreadLocalLogger::StopAndDrainFlusher() {
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    stop_requested_ = true;
  }
  work_cv_.notify_all();
  for (auto& lane : lanes_) {
    if (lane->flusher.joinable()) lane->flusher.join();
  }
}

EpochNumber ThreadLocalLogger::MinimumDurable() const {
  EpochNumber minimum = std::numeric_limits<EpochNumber>::max();
  for (const auto& lane : lanes_) {
    minimum = std::min(
        minimum, lane->durable.load(std::memory_order_seq_cst));
  }
  return minimum;
}

void ThreadLocalLogger::PublishMinimum() {
  std::lock_guard<std::mutex> lock(publish_mutex_);
  const EpochNumber minimum = MinimumDurable();
  if (minimum <= published_durable_) return;
  published_durable_ = minimum;
  publish_durable_(minimum);
}

void ThreadLocalLogger::FlusherLoop(size_t lane_id) {
  Lane& lane = *lanes_[lane_id];
  for (;;) {
    EpochNumber target = 0;
    {
      std::unique_lock<std::mutex> lock(state_mutex_);
      work_cv_.wait(lock, [this, &lane] {
        return stop_requested_ || failed_ ||
               pending_closed_ >
                   lane.durable.load(std::memory_order_seq_cst);
      });
      if (failed_) return;
      target = pending_closed_;
      const bool nothing_to_do =
          target <= lane.durable.load(std::memory_order_seq_cst);
      // Stop only once everything already closed is on the device, so a clean
      // shutdown does not drop records the tick had handed over.
      if (stop_requested_ && nothing_to_do) return;
      if (nothing_to_do) continue;
    }

    // The file and the per-node buffers are touched with no lock held, so a
    // committing thread never waits behind serialization or fdatasync.
    WalAppendResult result;
    try {
      result = FlushThrough(lane_id, lane, target);
    } catch (const std::exception& e) {
      SPDLOG_CRITICAL("Durability Error: the flusher threw: {0}", e.what());
      result = {false, EIO};
    } catch (...) {
      SPDLOG_CRITICAL("Durability Error: the flusher threw");
      result = {false, EIO};
    }

    if (!result.ok) {
      bool first_failure = false;
      {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (!failed_) {
          failed_ = true;
          first_failure = true;
        }
      }
      work_cv_.notify_all();
      if (first_failure) publish_failure_(result.error_number);
      return;
    }
    auto& trace                 = FlushTrace::Instance();
    const bool traced           = trace.Enabled();
    const int64_t publish_enter = traced ? FlushTrace::Now() : 0;
    lane.durable.store(target, std::memory_order_seq_cst);
    PublishMinimum();
    if (traced) trace.GroupPublish(target, publish_enter, FlushTrace::Now());
  }
}

WalAppendResult ThreadLocalLogger::FlushThrough(size_t lane_id, Lane& lane,
                                                EpochNumber target) {
  const EpochNumber durable_before =
      lane.durable.load(std::memory_order_seq_cst);
  FlushTrace::Instance().GroupCollectBegin(
      durable_before, static_cast<uint32_t>(lane_id));

  nodes_.ForEach([&, lane_id](ThreadLocalStorageNode* node) {
    if (node->lane_id.load(std::memory_order_acquire) != lane_id) return;
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
      lane.carry[record.epoch].emplace_back(std::move(record));
    }
  });

  FlushTrace::Instance().GroupCollectEnd();

  const auto result = lane.wal.AppendGroup(lane.carry, target);
  if (!result.ok) return result;
  // Buckets above the target stay for the next group; the ones just written are
  // the only ones dropped.
  lane.carry.erase(lane.carry.begin(), lane.carry.upper_bound(target));
  return result;
}

}  // namespace Recovery
}  // namespace LineairDB
