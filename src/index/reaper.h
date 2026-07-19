#ifndef LINEAIRDB_INDEX_REAPER_H
#define LINEAIRDB_INDEX_REAPER_H

#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "types/data_item.hpp"
#include "types/definitions.h"
#include "types/snapshot.hpp"
#include "types/transaction_id.hpp"

namespace LineairDB {
namespace Index {

class ConcurrentTable;
class SecondaryIndex;

/**
 * @brief Physically erases logically deleted index slots after a grace
 * period.
 *
 * A committed delete leaves its DataItem in the index as a tombstone so the
 * slot keeps its TID continuity for validation; the physical Purge runs
 * here, at least one full epoch after the delete committed. Committers
 * Enqueue candidates while still holding their commit lock, and the epoch
 * callback drives Reap, following the CallbackManager pattern of
 * enqueue-now, execute-on-epoch-advance. The shape mirrors the garbage
 * collection subsystem of the Silo reference implementation (its reaper
 * queue and last-reaped-epoch bookkeeping).
 */
class Reaper {
 public:
  /**
   * @brief Registers one logically deleted slot for a later physical purge.
   *
   * External-path form: the committer passes the already resolved owning
   * index, exactly one of primary_index / secondary_index. The call is a
   * no-op when `item` is null, `delete_commit_tid` is empty, or neither
   * index is given.
   */
  void Enqueue(ConcurrentTable* primary_index, SecondaryIndex* secondary_index,
               std::string_view key, DataItem* item,
               TransactionId delete_commit_tid);

  /**
   * @brief Native-path form: derives the target index from a committed
   * write-set Snapshot and enqueues only when it encodes a primary or
   * secondary delete.
   */
  void Enqueue(const Snapshot& snapshot, TransactionId delete_commit_tid);

  /**
   * @brief Purge every candidate whose delete epoch lies more than one full
   * epoch behind `published_epoch`.
   *
   * Candidates whose slot is locked are requeued; candidates whose slot was
   * reused or republished are dropped. Runs on the epoch-framework thread.
   */
  void Reap(EpochNumber published_epoch);

  /** Outcome of an absent-read purge-history check. */
  enum class AbsentReadCheck { Ok, PurgedAfterRead, HistoryExpired };

  /**
   * @brief Check whether a primary slot for `key` was physically purged by a
   * delete that committed at or after `read_epoch`.
   *
   * A no-slot observation at validation certifies "the key never existed"
   * only when no purge could have erased a post-read insert+delete round
   * trip. `HistoryExpired` means `read_epoch` predates the retained history
   * and the caller must fail closed.
   */
  AbsentReadCheck CheckAbsentRead(const void* index_identity,
                                  std::string_view key,
                                  EpochNumber read_epoch);

 private:
  /**
   * @brief Record one physically purged primary slot in the bounded history.
   */
  void RecordPurged(const void* index_identity, std::string_view key,
                    EpochNumber delete_epoch, EpochNumber published_epoch);

  /** Drop history buckets older than the retention window. */
  void PrunePurgeHistory(EpochNumber published_epoch);

  static std::string PurgeHistoryKey(const void* index_identity,
                                     std::string_view key);

  /** Which index owns the candidate's slot. */
  enum class DeferredPurgeIndexKind { Primary, Secondary };

  /**
   * @brief One logically deleted slot awaiting its physical purge.
   *
   * Exactly one of primary_index / secondary_index is set, selected by
   * `kind`. `item` is the slot pointer observed at enqueue time and serves
   * as an identity check at reap time. `delete_commit_tid` is the TID the
   * deleting commit published on the slot; it acts both as the grace-period
   * clock and as evidence that the slot still holds the deleted version.
   */
  // Physically purged slots (primary and secondary), retained for a
  // bounded epoch window.
  // purge_history_ maps slot identity to the newest purged delete epoch;
  // buckets are keyed by the publishing epoch for pruning. Reads older than
  // purge_history_horizon_ cannot be certified and fail closed.
  static constexpr EpochNumber kPurgeHistoryRetentionEpochs = 1024;
  std::mutex purge_history_mtx_;
  std::unordered_map<std::string, EpochNumber> purge_history_;
  std::deque<std::pair<EpochNumber,
                       std::vector<std::pair<std::string, EpochNumber>>>>
      purge_history_buckets_;
  EpochNumber purge_history_horizon_ = 0;

  struct DeferredPurgeCandidate {
    DeferredPurgeIndexKind kind;
    ConcurrentTable* primary_index = nullptr;
    SecondaryIndex* secondary_index = nullptr;
    std::string key;
    DataItem* item = nullptr;
    TransactionId delete_commit_tid;
  };

  /**
   * @brief Const-correct TID equality.
   *
   * TransactionId::operator== is not const-qualified upstream, so it cannot
   * be called with a const left-hand side; this helper compares the same
   * two fields.
   */
  static bool SameTransactionId(const TransactionId& lhs,
                                const TransactionId& rhs) {
    return lhs.epoch == rhs.epoch && lhs.tid == rhs.tid;
  }

  /**
   * @brief Re-resolves the candidate's key in its owning index.
   *
   * Returns the DataItem currently installed under the key, or nullptr.
   * Reap compares the result with `candidate.item`: a mismatch means the
   * slot was already purged and re-created, so the candidate is stale.
   */
  DataItem* ResolveDeferredPurgeCandidate(
      const DeferredPurgeCandidate& candidate);

  /**
   * @brief Physically erases the slot through the owning index's Purge.
   *
   * `retired_tid` (delete TID + 2, lock bit clear) is stamped on the erased
   * slot so an in-place reuse continues the slot's TID sequence instead of
   * restarting below the delete TID.
   */
  bool PurgeDeferredPurgeCandidate(const DeferredPurgeCandidate& candidate,
                                   TransactionId retired_tid);

  /** Guards the queue: Enqueue runs on committers, Reap on the epoch
   * thread. */
  std::mutex deferred_purge_mtx_;
  std::vector<DeferredPurgeCandidate> deferred_purge_candidates_;
  /** Cumulative totals for the debug log emitted by Reap. */
  uint64_t deferred_purge_reaped_ = 0;
  uint64_t deferred_purge_requeued_ = 0;
  uint64_t deferred_purge_dropped_ = 0;
};

}  // namespace Index
}  // namespace LineairDB

#endif  // LINEAIRDB_INDEX_REAPER_H
