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
 * @brief Owns the write-ahead log and the durability frontier.
 *
 * @details The frontier is the highest epoch whose records are on the
 * device. It only ever advances, and only after the fdatasync that made
 * those records durable returned successfully (an epoch with no records
 * advances it without one); a commit that waits on its own epoch therefore
 * learns the truth rather than an intention.
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

  // The record itself lives at namespace scope so that persistence code can
  // name it without depending on this interface; these aliases keep the
  // nested names that existing callers use.
  using LogRecord = Recovery::LogRecord;
  using LogRecords = Recovery::LogRecords;

  explicit Logger(const Config&, WalIo io = WalIo::Posix());
  ~Logger();

  /** See LoggerBase::Enqueue. */
  bool Enqueue(const WriteSetType& ws_ref, EpochNumber epoch);

  /**
   * @brief Reads the log, repairs an interrupted tail, initializes the
   * frontier, and returns the write set to replay.
   * @note Runs before the flusher starts and before the database accepts
   * work.
   */
  RecoveryResult Recover();

  /**
   * @brief Starts the flusher. Must follow Recover() and precede the first
   * tick.
   */
  void StartFlusher();

  /** See LoggerBase::ScheduleFlush. */
  void ScheduleFlush(EpochNumber closed);

  EpochNumber GetDurableEpoch() const {
    return durable_epoch_.load(std::memory_order_seq_cst);
  }

  /** See LoggerBase::WalFrontier. */
  EpochNumber GetWalFrontier() const { return logger_->WalFrontier(); }

  /**
   * @brief Blocks until the frontier reaches `commit_epoch`.
   * @details Pass Deadline::max() to wait without a timeout; shutdown and an
   * I/O failure still end the wait, as Stopped and Failed respectively. An
   * epoch that is already durable is reported as such even after a terminal
   * state.
   * @note The caller must have left its epoch first: waiting while online
   * would hold the epoch that has to close before the wait can end.
   */
  WaitResult WaitUntilDurable(EpochNumber commit_epoch, Deadline deadline);

  /**
   * @brief Publishes the acknowledgement policy that commits capture from
   * GetCommitDurability() after this returns.
   * @note Ordering across the switch is the caller's: this only publishes.
   * Database::SetCommitDurability is what pairs it with a durable barrier.
   */
  void SetCommitDurability(Config::CommitDurability mode);

  Config::CommitDurability GetCommitDurability() const;

  /**
   * @brief Returns once the transaction that committed in `commit_epoch` may
   * be acknowledged: at once when `awaits_durability` is false, and after
   * `commit_epoch` is durable when it is true.
   * @details The decision is the caller's, not this method's, so that a
   * policy switch cannot land between the caller's callback placement and
   * this wait and make the two disagree. A commit whose record cannot be made
   * durable stops the process: it has passed its serialization point, so an
   * abort would be a lie and an acknowledgement would be the lie the contract
   * exists to prevent.
   * @note The caller must have left its epoch, as WaitUntilDurable requires.
   */
  void AwaitCommitDurability(EpochNumber commit_epoch, bool awaits_durability);

  /**
   * @brief True while every closed epoch handed to the flusher is durable,
   * and unconditionally after a write failure: nothing further will be
   * written. Records buffered for epochs that have not closed yet do not
   * count.
   */
  bool IsQuiescent();

  /**
   * @brief Flushes everything already closed, then stops and joins the
   * flusher. After a write failure nothing more is flushed and the join is
   * immediate.
   */
  void StopAndDrainFlusher();

  /**
   * @brief Makes an I/O failure stop the process, once the log is the
   * durability contract's foundation rather than a component under test.
   * @details A logger that cannot write has no way to make later commits
   * durable, and under Async nobody waits to be told: the process would keep
   * acknowledging commits that are only in memory, and a measurement taken
   * after that point would describe a contract the run was no longer
   * honouring.
   * @note Armed explicitly, so a test that constructs a Logger directly can
   * still observe the failure state instead of dying with it.
   */
  void EnableProcessFailStop();

 private:
  void PublishDurable(EpochNumber frontier);
  void PublishFailure(int error_number);
  void PublishStopped();

  const std::string work_dir_;
  // Switchable at run time; see SetCommitDurability.
  std::atomic<Config::CommitDurability> durability_;
  // Whether this instance replays what it reads, which is what decides
  // whether a checkpoint image is read at all.
  const bool replays_;
  std::atomic<EpochNumber> durable_epoch_{0};

  enum class State { Running, Stopped, Failed };
  mutable std::mutex durability_mutex_;
  std::condition_variable durability_cv_;
  State state_{State::Running};
  int failure_errno_{0};
  bool process_fail_stop_{false};

  // Declared last: the backend's flusher publishes through the members above,
  // so it must be destroyed before them.
  std::unique_ptr<LoggerBase> logger_;
};

}  // namespace Recovery
}  // namespace LineairDB
#endif /* LINEAIRDB_RECOVERY_LOGGER_H */
