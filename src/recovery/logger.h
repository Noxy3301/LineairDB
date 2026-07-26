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
#ifndef LINEAIRDB_RECOVERY_LOGGER_H
#define LINEAIRDB_RECOVERY_LOGGER_H

#include <lineairdb/config.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>

#include "log_record.h"
#include "logger_base.h"
#include "types/data_buffer.hpp"
#include "types/definitions.h"
#include "wal.h"

namespace LineairDB {
namespace Recovery {

/**
 * Owns the write-ahead log and the durability frontier.
 *
 * The frontier is the highest epoch whose records are on the device. It only
 * ever advances, and only after the fdatasync that made those records durable
 * returned successfully; a commit that waits on its own epoch therefore learns
 * the truth rather than an intention.
 */
class Logger {
 public:
  constexpr static EpochNumber NumberIsNotUpdated = 0;

  using Deadline = std::chrono::steady_clock::time_point;

  enum class WaitResult {
    Durable,   // the frontier reached the requested epoch
    TimedOut,  // the deadline passed first
    Stopped,   // the logger shut down before reaching it
    Failed,    // the log could not be written
  };

  enum class RecoveryStatus { Ok, Failed };

  struct RecoveryResult {
    RecoveryStatus status{RecoveryStatus::Ok};
    EpochNumber frontier{0};
    WriteSetType recovery_set;
  };

  // The record itself lives at namespace scope so the WAL codec can name it
  // without depending on this interface; these aliases keep the nested names
  // that existing callers use.
  using LogRecord = Recovery::LogRecord;
  using LogRecords = Recovery::LogRecords;

  explicit Logger(const Config&, WalIo io = WalIo::Posix());
  ~Logger();

  /** See LoggerBase::Enqueue. */
  bool Enqueue(const WriteSetType& ws_ref, EpochNumber epoch);

  /**
   * Reads the log, repairs an interrupted tail, initializes the frontier, and
   * returns the write set to replay. Runs before the flusher starts and before
   * the database accepts work.
   */
  RecoveryResult Recover();

  /** Starts the flusher. Must follow Recover() and precede the first tick. */
  void StartFlusher();

  /** See LoggerBase::ScheduleFlush. */
  void ScheduleFlush(EpochNumber closed);

  EpochNumber GetDurableEpoch() const {
    return durable_epoch_.load(std::memory_order_seq_cst);
  }

  /**
   * Blocks until the frontier reaches `commit_epoch`. The caller must have left
   * its epoch first: waiting while online would hold the epoch that has to
   * close before the wait can end.
   *
   * Pass Deadline::max() to wait without a timeout; shutdown and an I/O failure
   * still end the wait, as Stopped and Failed respectively. An epoch that is
   * already durable is reported as such even after a terminal state.
   */
  WaitResult WaitUntilDurable(EpochNumber commit_epoch, Deadline deadline);

  /**
   * Returns once the transaction that committed in `commit_epoch` may be
   * acknowledged under the configured durability contract: at once unless the
   * contract is Sync and this transaction enqueued a record, and after
   * `commit_epoch` is durable when it did. The caller must have left its epoch,
   * as WaitUntilDurable requires.
   *
   * `log_enqueued` is the result of this transaction's Enqueue rather than
   * "the transaction wrote something": a write that the concurrency control
   * omitted leaves no record, and its epoch may never be written at all.
   *
   * A Sync commit whose record cannot be made durable stops the process. It has
   * already passed its serialization point, so reporting an abort would be a
   * lie, and acknowledging it would be the lie the contract exists to prevent.
   */
  void AwaitCommitDurability(EpochNumber commit_epoch, bool log_enqueued);

  /** True while nothing is waiting to be written. */
  bool IsQuiescent();

  /** Flushes everything already closed, then stops and joins the flusher. */
  void StopAndDrainFlusher();

 private:
  void PublishDurable(EpochNumber frontier);
  void PublishFailure(int error_number);
  void PublishStopped();

  const std::string work_dir_;
  const Config::CommitDurability durability_;
  std::atomic<EpochNumber> durable_epoch_{0};

  enum class State { Running, Stopped, Failed };
  mutable std::mutex durability_mutex_;
  std::condition_variable durability_cv_;
  State state_{State::Running};
  int failure_errno_{0};

  // Declared last: the backend's flusher publishes through the members above,
  // so it must be destroyed before them.
  std::unique_ptr<LoggerBase> logger_;
};

}  // namespace Recovery
}  // namespace LineairDB
#endif /* LINEAIRDB_RECOVERY_LOGGER_H */
