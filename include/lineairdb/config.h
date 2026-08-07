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

#ifndef LINEAIRDB_CONFIG_H
#define LINEAIRDB_CONFIG_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>

namespace LineairDB {

/**
 * @brief
 * Configuration and options for LineairDB instances.
 */
struct Config {
  /**
   * @brief
   * The size of thread pool.
   *
   * Default: LineairDB allocates threads as many the return value of
   * std::thread::hardware_concurrency().
   */
  size_t max_thread = std::thread::hardware_concurrency();
  /**
   * @brief
   * The size of epoch duration (milliseconds). See [Tu13, Chandramouli18] to
   * get more details of epoch-based group commit. Briefly, LineairDB
   * concurrently processes transactions included in the same epoch duration. As
   * you set the larger duration to this paramter, you may get a higher
   * throughput. However, this setting results in the increase of average
   * response time.
   *
   * Default: 40ms.
   * @see [Tu13] https://dl.acm.org/doi/10.1145/2517349.2522713
   * @see [Chandramouli18]
   * https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf
   */
  size_t epoch_duration_ms = 40;

  enum ConcurrencyControl { Silo, SiloNWR, TwoPhaseLocking };
  /**
   * @brief
   * Set a concurrency control algorithm.
   * See LineairDB::Config::ConcurrencyControl for the enum options of this
   * configuration.
   *
   * Default: SiloNWR
   */
  ConcurrencyControl concurrency_control_protocol = SiloNWR;

  enum Logger { ThreadLocalLogger };
  /**
   * @brief
   * Set a logging algorithm.
   * See LineairDB::Config::Logger for the enum options of this
   * configuration.
   *
   * Default: ThreadLocalLogger
   */
  Logger logger = ThreadLocalLogger;

  enum IndexStructure { HashTableWithPrecisionLockingIndex, Masstree };
  /**
   * @brief
   * Set the type of index.
   * See LineairDB::Config::IndexStructure for the enum options of this
   * configuration.
   *
   * Default: Hash table with precision locking index
   */
  IndexStructure index_structure = HashTableWithPrecisionLockingIndex;

  /**
   * @brief
   * If true, tables may install PAX storage metadata and route newly-created
   * row payloads through PaxStore.
   *
   * Default: false.
   */
  bool enable_pax_storage = false;

  enum CallbackEngine { ThreadLocal };
  /**
   * @brief
   * Set the type of callback engine.
   * See LineairDB::Config::CallBackEngine for the enum options of this
   * configuration.
   *
   * Default: ThreadLocal
   */
  CallbackEngine callback_engine = ThreadLocal;

  /**
   * @brief
   * If true, LineairDB processes recovery at the instantiation.
   *
   * Default: true
   */
  bool enable_recovery = true;

  /**
   * @brief
   * When a committing transaction is told that it has committed, relative to
   * when its log record is durable.
   *
   * - Volatile
   *   - No logging at all. A commit is acknowledged once it passes
   *     validation; nothing is written and nothing is recoverable.
   * - Async
   *   - Logging, acknowledged at precommit. The record becomes durable
   *     behind the committer, so an acknowledged transaction can be lost by
   *     a crash that happens before its epoch is written.
   * - Sync
   *   - Logging, acknowledged only after the committer's own epoch is
   *     durable.
   *
   * The equivalent names elsewhere, to keep Async from being read as a faster
   * Sync:
   * - Sync
   *   - SQL Server: full durability
   *   - PostgreSQL: synchronous_commit=on
   *   - Oracle: COMMIT WAIT
   * - Async
   *   - SQL Server: delayed durability
   *   - PostgreSQL: synchronous_commit=off
   *   - Oracle: COMMIT NOWAIT
   * - Volatile
   *   - No production equivalent; it is the logging-disabled research
   *     baseline.
   *
   * Default: Async
   */
  enum class CommitDurability {
    Volatile,
    Async,
    Sync,
  };
  CommitDurability commit_durability = CommitDurability::Async;

  /**
   * @brief
   * How much of the write-ahead log is made writable in place at a time.
   *
   * @details
   * The log file is written out with zeroes to this size before any record
   * lands in it, and records are then written in place, so a commit's
   * fdatasync persists data and not the size, allocation, or extent-state
   * metadata a growing file drags in (the benefit is filesystem- and
   * device-specific). A log that grows past it is extended by the same
   * amount again: a granularity rather than a limit. Larger means fewer
   * synchronous extensions but a longer startup scan of the reserved
   * region. Zero disables reservation and lets the file grow as written,
   * which is what a Volatile database is given: it never writes a record.
   *
   * @note The storage stack must honour fsync/fdatasync; the supported
   * ext4 setup uses its default data ordering and barriers. A log from a
   * build that did not reserve is readable here; the reverse does not
   * hold (such a build reads the reserved zeroes as a broken frame and
   * refuses to start).
   *
   * Default: 64 MiB
   */
  uint64_t wal_initial_capacity_bytes = 64ull * 1024ull * 1024ull;

  /**
   * @brief
   * True while LineairDB performs logging for recovery.
   *
   * @deprecated Derived from commit_durability, which is the setting that
   * decides logging. The Database ignores the value set here and derives its
   * own stored copy from commit_durability. The field remains so that
   * existing code that reads it keeps compiling.
   */
  bool enable_logging = true;

  /**
   * @brief
   * Checkpointing (CPR-consistency [1]) is not implemented for the
   * epoch-frame write-ahead log: setting this to true is a startup error
   * rather than a silent no-op, and the log grows without truncation.
   *
   * Default: false
   * @ref [1]:
   * https://www.microsoft.com/en-us/research/uploads/prod/2019/01/cpr-sigmod19.pdf
   */
  bool enable_checkpointing = false;

  /**
   * @brief
   * It uses as the interval time (seconds) for checkpointing.
   * The longer is the better for the performance but the larger interval time
   * causes the increasing of log file size.
   *
   * Default: 30
   */
  size_t checkpoint_period = 30;

  /**
   * @brief
   * It uses as the threshold (percentage) for rehashing of the hash index.
   * A large value (e.g., 99) will not easily rehash the index and thus reduce
   * memory consumption because leaving less room in the index. On the other
   * hand, a problem with open addressing hash indexes (current implementation)
   * is that the computational cost of an insert increases on the less room.
   *
   * Default: 75 (percent)
   */
  double rehash_threshold = 0.75;

  /**
   * @brief
   * The directory path that lineardb use as working directory.
   * All of data, logs and related files are stored in the directory.
   *
   * Default: "lineairdb_logs"
   */
  std::string work_dir = "./lineairdb_logs";

  /**
   * @brief
   * The name of the anonymous table.
   * Anonymous table is used to store the data that is not associated with any
   * table.
   *
   * Default: "__anonymous_table"
   */
  std::string anonymous_table_name = "__anonymous_table";
};

// Secondary index options (moved from secondary_index_option.h)
struct SecondaryIndexOption {
  enum class Constraint : unsigned {
    NONE = 0u,
    UNIQUE = 1u << 0,
    NOT_NULL = 1u << 1,
  };
};

// bitwise operators for enum class Constraint
constexpr SecondaryIndexOption::Constraint operator|(
    SecondaryIndexOption::Constraint a, SecondaryIndexOption::Constraint b) {
  using U = std::underlying_type_t<SecondaryIndexOption::Constraint>;
  return static_cast<SecondaryIndexOption::Constraint>(static_cast<U>(a) |
                                                       static_cast<U>(b));
}

constexpr SecondaryIndexOption::Constraint operator&(
    SecondaryIndexOption::Constraint a, SecondaryIndexOption::Constraint b) {
  using U = std::underlying_type_t<SecondaryIndexOption::Constraint>;
  return static_cast<SecondaryIndexOption::Constraint>(static_cast<U>(a) &
                                                       static_cast<U>(b));
}

constexpr bool HasFlag(SecondaryIndexOption::Constraint set,
                       SecondaryIndexOption::Constraint flag) {
  using U = std::underlying_type_t<SecondaryIndexOption::Constraint>;
  return (static_cast<U>(set) & static_cast<U>(flag)) != 0;
}
}  // namespace LineairDB

#endif
