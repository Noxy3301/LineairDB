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

#ifndef LINEAIRDB_DATABASE_H
#define LINEAIRDB_DATABASE_H

#include <lineairdb/config.h>
#include <lineairdb/stateless.h>
#include <lineairdb/transaction.h>

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "config.h"
#include "tx_status.h"

namespace LineairDB {

class Database {
 public:
  /**
   * @brief Construct a new Database object. Thread-safe.
   * Note that a default-constructed Config object will be passed.
   */
  Database() noexcept;

  /**
   * @brief Construct a new Database object. Thread-safe.
   * @param config See Config for more details of configuration.
   */
  Database(const Config& config) noexcept;

  ~Database() noexcept;
  Database(const Database&) = delete;
  Database& operator=(const Database&) = delete;
  Database(Database&&) = delete;
  Database& operator=(Database&&) = delete;

  /**
   * @brief Return the Config object set by constructor.
   * @return Config object. Note that it is not an lvalue reference
   * and you cannot change the configuration with it.
   * Thread-safe.
   */
  const Config GetConfig() const noexcept;

  using ProcedureType = std::function<void(Transaction&)>;
  using CallbackType = std::function<void(const TxStatus)>;
  /**
   * @brief
   * Processes a transaction given by a transaction procedure proc,
   * and afterwards process callback function with the resulting TxStatus.
   * It enqueues these two functions into LineairDB's thread pool.
   * Thread-safe.
   * @param[in] proc A transaction procedure processed by LineairDB.
   * @param[out] commit_clbk A callback function accepts a result (Committed or
   * Aborted) of this transaction.
   * @param[out] precommit_clbk A callback function accepts a result
   * (Precommitted or Aborted) of this transaction.
   * Note that pre-committed transactions have not been committed. Since the
   * recovery log has not been persisted, this transaction may be aborted. The
   * callback is good for describing  transaction dependencies. If a transaction
   * is aborted, it is guaranteed that the other  transactions, that are
   executed
   * after checking the pre-commit of the transaction, will abort.
   */
  void ExecuteTransaction(
      ProcedureType proc, CallbackType commit_clbk,
      std::optional<CallbackType> precommit_clbk = std::nullopt);

  /**
   * @brief
   * Creates a new transaction.
   * Via this interface, the callee thread of this method can manipulate
   * LineairDB's key-value storage directly. Note that the behavior and
   * performance characteristics will affect from the selected callback manager
   * and log manager; For example, if you have set Config::CallbackManager to
   * ThreadLocal, the callee thread of this method may have to call
   * Database::RequestCommit more frequently, in order to resolve the congestion
   * of the thread-local commit callback queue.
   *
   * @return Transaction
   */
  Transaction& BeginTransaction();

  /**
   * @brief
   * Terminates the transaction.
   * If Transaction::Abort has not been called, LineairDB tries to commit `tx`.
   * @pre To achieve user abort, Transaction::Abort must be called before this
   * method.
   * @post The first argument `tx` might have been deleted.
   * @param[in] tx A transaction wants to terminate.
   * @param[out] clbk A callback function accepts a result (Committed or
   * @return true if the LineairDB's concurrency control protocol **decides** to
   * commit the given `tx`. Note that it does not mean that `tx has been
   * committed`; `tx` will be committed if it goes without crash, disaster, or
   * something accident, however, the commit cannot be determined until
   * recoverability is guaranteed by persistent logs. Use clbk to find out
   * whether you have really committed or not.
   * @return false if the LineairDB's concurrency control protocol decides to
   * abort the given `tx`. In contrast with the true case, this result will not
   * be overturned.
   */
  bool EndTransaction(Transaction& tx, CallbackType clbk);

  /**
   * @brief
   * Fence() waits termination of transactions which is currently in progress.
   * You can execute transactions in the order you want by interleaving Fence()
   * between ExecuteTransaction functions. Note that no Fence() call may result
   * in a execution sequence which is not same as the program (invoking) order
   * of the ExecuteTransaction functions. If you know some dependency of
   * transactions (e.g., database population), use this method to order
   * them. Thread-safe.
   */
  void Fence() const noexcept;

  /**
   * @brief
   * WaitForCheckpoint
   * Waits for the completion of the next checkpoint.
   * Note that this method may take longer than the time specified in
   * `LineairDB::Config.checkpoint_period (default value: 30 seconds)` in the
   * worst cases. When the WAL (logging) is disabled and durability is
   * guaranteed by only checkpointing, this interface is preferable; it ensures
   *that the currently active transactions are durable.
   */
  void WaitForCheckpoint() const noexcept;

  /**
   * @brief
   * Requests executions of callback functions of already completed (committed
   * or (aborted) transactions. Note that LineairDB's callback queues may be
   * overloading in some combination of configurations (e.g., too long epoch
   * size, too small thread-pool size, or weird implementation of the selected
   * CallbackManager).
   */
  void RequestCallbacks();

  bool CreateSecondaryIndex(const std::string_view table_name,
                            const std::string_view index_name,
                            const uint index_type);

  /**
   * @brief
   * Creates a new table.
   * @param[in] table_name The name of the table to create.
   * @return true when a new table is created (the name was not previously
   * used).
   * @return false when no table is created because the table name already
   * exists.
   */
  bool CreateTable(const std::string_view table_name);

  // ----------------------------------------------------------------------
  // Stateless read / validate-and-commit API.
  //
  // The methods below do not allocate a server-side Transaction. Each call
  // returns the snapshot the caller needs (value, packed TID, observed
  // Masstree node versions) so that the caller can keep its own read set
  // across independent RPCs. The collected snapshot is replayed through
  // ValidateAndCommit when the logical transaction is ready to commit.
  // See @ref stateless.h for the supporting types.
  // ----------------------------------------------------------------------

  /**
   * @brief Read one row without opening a server-side transaction.
   *
   * Looks the key up in the primary index of `table_name` and returns the
   * current value together with the packed TID observed at read time. The
   * caller should later pass the same TID back inside an ExternalReadEntry
   * so that ValidateAndCommit can confirm the row was not modified
   * concurrently.
   *
   * @param table_name Target table.
   * @param key Primary key to look up.
   * @return Result with `found` set when the key exists and was non-empty.
   *         When the table does not exist, `found` is false and `tid` is 0.
   */
  StatelessReadResult StatelessRead(const std::string_view table_name,
                                    const std::string_view key);

  /**
   * @brief Read several rows in one call.
   *
   * Each `keys[i] = {table_name, key}` is resolved with the same protocol as
   * StatelessRead. Reads do not share state, so this is purely a transport
   * optimization on top of repeated StatelessRead calls.
   *
   * @param keys (table_name, key) pairs to look up.
   * @return One StatelessReadResult per input, in the same order.
   */
  std::vector<StatelessReadResult> StatelessBatchRead(
      const std::vector<std::pair<std::string, std::string>>& keys);

  /**
   * @brief Range-scan the primary index and return all rows together with
   *        the validation tokens needed to revalidate the range at commit.
   *
   * Each returned row carries its own TID. The result also includes the
   * Masstree node versions touched by the scan (range_versions) and any
   * exact tombstone entries (index_reads), so a concurrent insert that
   * reuses a tombstone slot or splits a leaf is detected by
   * ValidateAndCommit.
   *
   * @param table_name Target table.
   * @param start_key Inclusive start of the range.
   * @param end_key   Exclusive end of the range. Must be non-empty.
   * @param row_limit Maximum rows to return. 0 means no cap.
   * @param reverse_scan When true, iterate from `end_key` toward `start_key`.
   * @return Result with `ok == false` if the scan retried out or the table
   *         is missing. Callers should treat `!ok` as an abort signal.
   */
  StatelessRangeScanResult StatelessRangeScan(
      const std::string_view table_name, const std::string_view start_key,
      const std::string_view end_key, uint64_t row_limit, bool reverse_scan);

  /**
   * @brief Range-scan a secondary index and resolve each hit to its base row.
   *
   * For every secondary key in `[start_key, end_key)`, this resolves each of
   * its primary keys, reads the base row, and reports
   * `{secondary_key, primary_key, value, tid, found}` per result. Validation
   * tokens cover both the secondary index range and each base-row read.
   *
   * @param table_name Base table.
   * @param index_name Secondary index name.
   * @param start_key Inclusive start of the secondary range.
   * @param end_key Exclusive end of the secondary range. Must be non-empty.
   * @param row_limit Maximum rows to return. 0 means no cap.
   * @param reverse_scan When true, iterate in reverse secondary-key order.
   * @return Result with `ok == false` if the scan retried out or the
   *         table/index is missing.
   */
  StatelessSecondaryRangeScanResult StatelessSecondaryRangeScan(
      const std::string_view table_name, const std::string_view index_name,
      const std::string_view start_key, const std::string_view end_key,
      uint64_t row_limit, bool reverse_scan);

  /**
   * @brief Validate a caller-collected snapshot and install its writes
   *        atomically.
   *
   * Runs the Silo commit phase against external inputs:
   *   1. Resolve each read/write/SI op to its DataItem.
   *   2. Lock every write target.
   *   3. Re-check `reads` and `index_reads` TIDs against the locked state.
   *   4. Re-check `range_reads` either by Masstree node version
   *      (physical form) or by replaying the scan (logical form).
   *   5. Re-check UNIQUE secondary-index adds against the locked SI slots.
   *   6. Install writes, append the log set, and unlock with a new TID.
   *
   * Aborts return false. The optional `abort_reason` is set to a short
   * machine-readable label such as `exact_read_tid_moved`,
   * `range_node_version_changed`, or `unique_si_exists_after_lock`.
   *
   * @param reads Point reads to revalidate before commit.
   * @param writes Row writes (`is_delete == true` to remove the row).
   * @param secondary_index_ops Secondary-index adds/removes to install.
   * @param range_reads Range validation tokens collected by earlier scans.
   * @param index_reads Exact-key tokens collected alongside scans.
   * @param abort_reason Optional out parameter. Set only when the function
   *                    returns false.
   * @return true on commit; false on validation failure or schema mismatch.
   */
  bool ValidateAndCommit(
      const std::vector<ExternalReadEntry>& reads,
      const std::vector<ExternalWriteEntry>& writes,
      const std::vector<ExternalSecondaryIndexEntry>& secondary_index_ops,
      const std::vector<ExternalRangeValidationEntry>& range_reads = {},
      const std::vector<ExternalIndexValidationEntry>& index_reads = {},
      std::string* abort_reason = nullptr);

  class Impl;

 private:
  const std::unique_ptr<Impl> db_pimpl_;
  friend class Transaction;
};
};  // namespace LineairDB

#endif
