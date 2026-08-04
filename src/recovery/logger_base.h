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

#ifndef LINEAIRDB_RECOVERY_LOGGER_BASE_H
#define LINEAIRDB_RECOVERY_LOGGER_BASE_H

#include "log_record.h"
#include "types/data_item.hpp"
#include "types/definitions.h"
#include "types/snapshot.hpp"
#include "wal.h"

namespace LineairDB {
namespace Recovery {

class LoggerBase {
 public:
  virtual ~LoggerBase() {}

  /**
   * @brief Buffers one committed transaction's write set.
   * @return Whether anything was buffered: a transaction whose write set
   * produces no key-value pair has nothing to make durable, and the commit
   * path must not wait for it.
   */
  virtual bool Enqueue(const WriteSetType& ws_ref, EpochNumber epoch) = 0;

  /**
   * @brief Reads and repairs the log. Must complete before the flusher
   * starts.
   */
  virtual WalScanResult ScanAndRepairWal() = 0;

  /** @brief Starts the flusher. Called once, after the log has been
   * scanned. */
  virtual void StartFlusher() = 0;

  /**
   * @brief Publishes a new closed epoch.
   * @note Called from the epoch writer thread; only records at or below
   * `closed` may be written, because a later epoch can still gain
   * participants.
   */
  virtual void ScheduleFlush(EpochNumber closed) = 0;

  /**
   * @brief Flushes everything already closed, then stops and joins the
   * flusher. After a write failure nothing more is flushed and the join is
   * immediate.
   */
  virtual void StopAndDrainFlusher() = 0;

  /**
   * @brief True while every closed epoch handed over is durable, and
   * unconditionally after a write failure: nothing further will be written.
   */
  virtual bool IsQuiescent() = 0;
};

}  // namespace Recovery
}  // namespace LineairDB

#endif /* LINEAIRDB_RECOVERY_LOGGER_BASE_H */
