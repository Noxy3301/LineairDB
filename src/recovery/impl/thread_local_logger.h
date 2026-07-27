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
#include <optional>
#include <thread>

#include "recovery/flush_trace.h"
#include "recovery/log_record.h"
#include "recovery/logger_base.h"
#include "recovery/wal.h"
#include "types/definitions.h"
#include "util/thread_key_storage.h"

namespace LineairDB {
namespace Recovery {

/**
 * Buffers log records per producing thread and drains them through two ordered
 * stages: one preparer and one WAL I/O thread.
 *
 * Producers never touch the file: a committing thread appends to its own
 * thread-local vector under a short lock and leaves. The preparer swaps those
 * vectors, buckets and encodes the records by epoch, then hands immutable bytes
 * to the I/O stage. The queue has depth one: group N+1 can be prepared while
 * group N waits in fdatasync, but writes and publication remain strictly ordered.
 *
 * The stages get their own threads rather than slots in the shared pool because
 * a pool worker only serves the no-steal queue that carries visibility
 * callbacks when its work queue is empty; a flusher that always has a group
 * ready would postpone those callbacks indefinitely, and with them Fence.
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
  struct ThreadLocalStorageNode {
    std::mutex log_records_mutex;
    LogRecords log_records;
  };

  struct PreparedFlush {
    EpochNumber target{0};
    WalEncodedGroup encoded;
    FlushTrace::GroupRow trace;
  };

  void PreparerLoop();
  void FlusherLoop();
  /** Swaps every node's buffer, buckets by epoch, and encodes buckets <= target. */
  WalAppendResult PrepareThrough(EpochNumber target,
                                 EpochNumber prepared_before,
                                 PreparedFlush* prepared);
  void Fail(int error_number);

  ThreadKeyStorage<ThreadLocalStorageNode> nodes_;

  // Owned by the preparer thread alone, between StartFlusher and the join.
  std::map<EpochNumber, LogRecords> carry_;
  Wal wal_;

  PublishDurable publish_durable_;
  PublishFailure publish_failure_;
  ReadDurable read_durable_;

  std::mutex state_mutex_;
  std::condition_variable work_cv_;
  EpochNumber pending_closed_{0};
  EpochNumber prepared_through_{0};
  bool stop_requested_{false};
  bool failed_{false};
  bool preparer_done_{false};
  std::optional<PreparedFlush> prepared_;
  std::thread preparer_;
  std::thread flusher_;
};

}  // namespace Recovery
}  // namespace LineairDB
#endif /* LINEAIRDB_RECOVERY_THREAD_LOCAL_LOGGER_H */
