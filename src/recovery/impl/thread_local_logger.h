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
#ifndef LINEAIRDB_RECOVERY_THREAD_LOCAL_LOGGER_H
#define LINEAIRDB_RECOVERY_THREAD_LOCAL_LOGGER_H

#include <lineairdb/config.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "recovery/log_record.h"
#include "recovery/logger_base.h"
#include "recovery/wal.h"
#include "types/definitions.h"
#include "util/thread_key_storage.h"

namespace LineairDB {
namespace Recovery {

/**
 * Buffers log records per producing thread and writes them through one or more
 * independent WAL lanes.
 *
 * Producers never touch the file: a committing thread appends to its own
 * thread-local vector under a short lock and leaves. Each producer is assigned
 * to one lane for its lifetime. A lane's flusher swaps only those vectors,
 * buckets the records by epoch, and writes one group per fdatasync.
 *
 * Flushers get their own threads rather than slots in the shared pool because
 * a pool worker only serves the no-steal queue that carries visibility
 * callbacks when its work queue is empty; a flusher that always has a group
 * ready would postpone those callbacks indefinitely, and with them Fence.
 *
 * A closed epoch becomes globally durable only after every lane has processed
 * it. An empty lane advances without I/O; an active lane advances only after
 * its own fdatasync.
 */
class ThreadLocalLogger final : public LoggerBase {
 public:
  using PublishDurable = std::function<void(EpochNumber)>;
  using PublishFailure = std::function<void(int)>;
  using ReadDurable = std::function<EpochNumber()>;

  ThreadLocalLogger(const Config&, PublishDurable, PublishFailure, ReadDurable,
                    WalIo io = WalIo::Posix());
  ~ThreadLocalLogger() override;

  bool Enqueue(const WriteSetType& ws_ref, EpochNumber epoch) final override;
  WalScanResult ScanAndRepairWal() final override;
  void StartFlusher() final override;
  void ScheduleFlush(EpochNumber closed) final override;
  void StopAndDrainFlusher() final override;
  bool IsQuiescent() final override;

 private:
  static constexpr size_t kUnassignedLane = std::numeric_limits<size_t>::max();

  struct ThreadLocalStorageNode {
    std::atomic<size_t> lane_id{kUnassignedLane};
    std::mutex log_records_mutex;
    LogRecords log_records;
  };

  struct Lane {
    Lane(const std::string& work_dir, WalIo io, uint64_t capacity,
         const std::string& file_name)
        : wal(work_dir, std::move(io), capacity, file_name) {}

    std::map<EpochNumber, LogRecords> carry;
    Wal wal;
    std::atomic<EpochNumber> durable{0};
    std::thread flusher;
  };

  void FlusherLoop(size_t lane_id);
  /** Swaps this lane's node buffers, buckets by epoch, and writes through target. */
  WalAppendResult FlushThrough(size_t lane_id, Lane& lane, EpochNumber target);
  EpochNumber MinimumDurable() const;
  void PublishMinimum();

  ThreadKeyStorage<ThreadLocalStorageNode> nodes_;
  std::atomic<size_t> next_lane_{0};
  std::vector<std::unique_ptr<Lane>> lanes_;

  PublishDurable publish_durable_;
  PublishFailure publish_failure_;

  std::mutex publish_mutex_;
  EpochNumber published_durable_{0};

  std::mutex state_mutex_;
  std::condition_variable work_cv_;
  EpochNumber pending_closed_{0};
  bool stop_requested_{false};
  bool failed_{false};
};

}  // namespace Recovery
}  // namespace LineairDB
#endif /* LINEAIRDB_RECOVERY_THREAD_LOCAL_LOGGER_H */
