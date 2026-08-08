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

#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <thread>

#include "recovery/log_record.h"
#include "recovery/logger_base.h"
#include "recovery/wal.h"
#include "types/definitions.h"
#include "util/thread_key_storage.h"

namespace LineairDB {
namespace Recovery {

/**
 * @brief Buffers log records per producing thread and writes them from one
 * dedicated flusher thread.
 *
 * @details Producers never touch the file: a committing thread appends to
 * its own thread-local vector under a short lock and leaves. The flusher
 * swaps those vectors, buckets the records by epoch, and writes one group
 * per fdatasync.
 *
 * @note The flusher owns a thread instead of a pool slot: a pool worker
 * serves the visibility-callback queue only while its own work queue is
 * empty, and a flusher there would postpone those callbacks, and Fence,
 * indefinitely.
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
  WalScanResult ScanAndRepairWal(EpochNumber min_epoch) final override;
  EpochNumber WalFrontier() const final override;
  void StartFlusher() final override;
  void ScheduleFlush(EpochNumber closed) final override;
  void StopAndDrainFlusher() final override;
  bool IsQuiescent() final override;

 private:
  struct ThreadLocalStorageNode {
    std::mutex log_records_mutex;
    LogRecords log_records;
  };

  void FlusherLoop();
  /** @brief Swaps every node's buffer, buckets by epoch, writes buckets at
   * or below `target`. */
  WalAppendResult FlushThrough(EpochNumber target);

  ThreadKeyStorage<ThreadLocalStorageNode> nodes_;

  // Owned by the flusher thread alone, between StartFlusher and the join.
  std::map<EpochNumber, LogRecords> carry_;
  Wal wal_;

  PublishDurable publish_durable_;
  PublishFailure publish_failure_;
  ReadDurable read_durable_;

  std::mutex state_mutex_;
  std::condition_variable work_cv_;
  EpochNumber pending_closed_{0};
  bool stop_requested_{false};
  bool failed_{false};
  std::thread flusher_;
};

}  // namespace Recovery
}  // namespace LineairDB
#endif /* LINEAIRDB_RECOVERY_THREAD_LOCAL_LOGGER_H */
