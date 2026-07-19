#include "index/reaper.h"

#include <iterator>
#include <utility>

#include "index/concurrent_table.h"
#include "index/impl/masstree_index.hpp"
#include "index/secondary_index.h"
#include "util/logger.hpp"

namespace LineairDB {
namespace Index {

void Reaper::Enqueue(ConcurrentTable* primary_index,
                                   SecondaryIndex* secondary_index,
                                   std::string_view key, DataItem* item,
                                   TransactionId delete_commit_tid) {
  if (item == nullptr || delete_commit_tid.IsEmpty()) return;
  if (primary_index == nullptr && secondary_index == nullptr) return;

  DeferredPurgeCandidate candidate;
  candidate.kind = secondary_index == nullptr
                       ? DeferredPurgeIndexKind::Primary
                       : DeferredPurgeIndexKind::Secondary;
  candidate.primary_index = primary_index;
  candidate.secondary_index = secondary_index;
  candidate.key = std::string(key);
  candidate.item = item;
  candidate.delete_commit_tid = delete_commit_tid;

  std::lock_guard<std::mutex> lk(deferred_purge_mtx_);
  deferred_purge_candidates_.emplace_back(std::move(candidate));
}

void Reaper::Enqueue(const Snapshot& snapshot,
                                   TransactionId delete_commit_tid) {
  const bool primary_delete = snapshot.index_name.empty() &&
                              snapshot.pi_ref != nullptr &&
                              !snapshot.data_item_copy.IsPrimaryInitialized();
  if (primary_delete) {
    Enqueue(snapshot.pi_ref, nullptr, snapshot.key, snapshot.index_cache,
            delete_commit_tid);
    return;
  }

  const bool secondary_delete = !snapshot.index_name.empty() &&
                                snapshot.si_ref != nullptr &&
                                snapshot.data_item_copy.primary_keys_view()
                                    .empty();
  if (secondary_delete) {
    Enqueue(nullptr, snapshot.si_ref, snapshot.key, snapshot.index_cache,
            delete_commit_tid);
  }
}

DataItem* Reaper::ResolveDeferredPurgeCandidate(
    const DeferredPurgeCandidate& candidate) {
  if (candidate.kind == DeferredPurgeIndexKind::Primary) {
    return candidate.primary_index == nullptr
               ? nullptr
               : candidate.primary_index->Get(candidate.key);
  }
  return candidate.secondary_index == nullptr
             ? nullptr
             : candidate.secondary_index->Get(candidate.key);
}

bool Reaper::PurgeDeferredPurgeCandidate(
    const DeferredPurgeCandidate& candidate, TransactionId retired_tid) {
  if (candidate.kind == DeferredPurgeIndexKind::Primary) {
    return candidate.primary_index != nullptr &&
           candidate.primary_index->Purge(candidate.key, candidate.item,
                                          retired_tid);
  }
  return candidate.secondary_index != nullptr &&
         candidate.secondary_index->Purge(candidate.key, candidate.item,
                                          retired_tid);
}

void Reaper::Reap(EpochNumber published_epoch) {
  PrunePurgeHistory(published_epoch);
  std::vector<DeferredPurgeCandidate> ready;
  size_t pending_before = 0;
  {
    std::lock_guard<std::mutex> lk(deferred_purge_mtx_);
    pending_before = deferred_purge_candidates_.size();
    std::vector<DeferredPurgeCandidate> pending;
    pending.reserve(deferred_purge_candidates_.size());
    for (auto& candidate : deferred_purge_candidates_) {
      const EpochNumber delete_epoch = candidate.delete_commit_tid.epoch;
      const bool one_full_epoch_elapsed =
          published_epoch > delete_epoch &&
          published_epoch - delete_epoch > 1;
      if (one_full_epoch_elapsed) {
        ready.emplace_back(std::move(candidate));
      } else {
        pending.emplace_back(std::move(candidate));
      }
    }
    deferred_purge_candidates_.swap(pending);
  }

  if (ready.empty()) {
    if (pending_before != 0) {
      SPDLOG_DEBUG(
          "Deferred purge epoch={} pending={} reaped=0 requeued=0 "
          "dropped=0 total_reaped={} total_requeued={} total_dropped={}",
          published_epoch, pending_before, deferred_purge_reaped_,
          deferred_purge_requeued_, deferred_purge_dropped_);
    }
    return;
  }

  std::vector<DeferredPurgeCandidate> requeue;
  requeue.reserve(ready.size());
  size_t reaped = 0;
  size_t requeued = 0;
  size_t dropped = 0;

  for (auto& candidate : ready) {
    DataItem* item = ResolveDeferredPurgeCandidate(candidate);
    if (item != candidate.item) {
      ++dropped;
      continue;
    }

    TransactionId observed = item->transaction_id.load();
    if (observed.tid & 1u) {
      requeue.emplace_back(std::move(candidate));
      ++requeued;
      continue;
    }
    if (!SameTransactionId(observed, candidate.delete_commit_tid)) {
      ++dropped;
      continue;
    }

    TransactionId locked = observed;
    locked.tid |= 1u;
    if (!item->transaction_id.compare_exchange_strong(observed, locked)) {
      if (observed.tid & 1u) {
        requeue.emplace_back(std::move(candidate));
        ++requeued;
      } else {
        ++dropped;
      }
      continue;
    }

    auto unlock_candidate = [&]() {
      item->transaction_id.store(candidate.delete_commit_tid);
    };

    const bool item_initialized =
        candidate.kind == DeferredPurgeIndexKind::Primary
            ? item->IsPrimaryInitialized()
            : item->IsInitialized();
    if (item_initialized) {
      unlock_candidate();
      ++dropped;
      continue;
    }
    if (ResolveDeferredPurgeCandidate(candidate) != item) {
      unlock_candidate();
      ++dropped;
      continue;
    }
    if (!SameTransactionId(item->transaction_id.load(), locked)) {
      unlock_candidate();
      ++dropped;
      continue;
    }

    TransactionId retired = candidate.delete_commit_tid;
    retired.tid = (retired.tid + 2u) & ~1u;
    // Publish the purge evidence BEFORE the physical removal: a validator
    // that observes the missing slot must already find the history entry.
    // A failed purge leaves a spurious entry behind, which only over-aborts.
    const void* index_identity =
        candidate.kind == DeferredPurgeIndexKind::Primary
            ? static_cast<const void*>(candidate.primary_index)
            : static_cast<const void*>(candidate.secondary_index);
    RecordPurged(index_identity, candidate.key,
                 candidate.delete_commit_tid.epoch, published_epoch);
    if (PurgeDeferredPurgeCandidate(candidate, retired)) {
      ++reaped;
    } else {
      unlock_candidate();
      ++dropped;
    }
  }

  [[maybe_unused]] size_t pending_after = 0;
  [[maybe_unused]] uint64_t total_reaped = 0;
  [[maybe_unused]] uint64_t total_requeued = 0;
  [[maybe_unused]] uint64_t total_dropped = 0;
  {
    std::lock_guard<std::mutex> lk(deferred_purge_mtx_);
    deferred_purge_candidates_.insert(deferred_purge_candidates_.end(),
                                      std::make_move_iterator(requeue.begin()),
                                      std::make_move_iterator(requeue.end()));
    deferred_purge_reaped_ += reaped;
    deferred_purge_requeued_ += requeued;
    deferred_purge_dropped_ += dropped;
    pending_after = deferred_purge_candidates_.size();
    total_reaped = deferred_purge_reaped_;
    total_requeued = deferred_purge_requeued_;
    total_dropped = deferred_purge_dropped_;
  }

  SPDLOG_DEBUG(
      "Deferred purge epoch={} pending={} reaped={} requeued={} dropped={} "
      "total_reaped={} total_requeued={} total_dropped={}",
      published_epoch, pending_after, reaped, requeued, dropped, total_reaped,
      total_requeued, total_dropped);

  MasstreeReleaseThreadEpoch();
}


std::string Reaper::PurgeHistoryKey(const void* index_identity,
                                    std::string_view key) {
  std::string history_key;
  history_key.reserve(sizeof(index_identity) + 1 + key.size());
  history_key.append(reinterpret_cast<const char*>(&index_identity),
                     sizeof(index_identity));
  history_key.push_back('\0');
  history_key.append(key.data(), key.size());
  return history_key;
}

void Reaper::RecordPurged(const void* index_identity, std::string_view key,
                          EpochNumber delete_epoch,
                          EpochNumber published_epoch) {
  std::lock_guard<std::mutex> lk(purge_history_mtx_);
  std::string history_key = PurgeHistoryKey(index_identity, key);
  auto [it, inserted] = purge_history_.try_emplace(history_key, delete_epoch);
  if (!inserted && it->second < delete_epoch) it->second = delete_epoch;
  if (purge_history_buckets_.empty() ||
      purge_history_buckets_.back().first != published_epoch) {
    purge_history_buckets_.push_back({published_epoch, {}});
  }
  purge_history_buckets_.back().second.emplace_back(std::move(history_key),
                                                    delete_epoch);
}

void Reaper::PrunePurgeHistory(EpochNumber published_epoch) {
  if (published_epoch <= kPurgeHistoryRetentionEpochs) return;
  const EpochNumber cutoff = published_epoch - kPurgeHistoryRetentionEpochs;
  std::lock_guard<std::mutex> lk(purge_history_mtx_);
  while (!purge_history_buckets_.empty() &&
         purge_history_buckets_.front().first <= cutoff) {
    for (auto& entry : purge_history_buckets_.front().second) {
      auto it = purge_history_.find(entry.first);
      if (it != purge_history_.end() && it->second == entry.second) {
        purge_history_.erase(it);
      }
    }
    purge_history_buckets_.pop_front();
  }
  // A pruned bucket at publishing epoch P held purges of deletes at epochs
  // <= P - 2, so every delete at cutoff + 1 or later is still retained.
  if (purge_history_horizon_ < cutoff + 1) {
    purge_history_horizon_ = cutoff + 1;
  }
}

Reaper::AbsentReadCheck Reaper::CheckAbsentRead(const void* index_identity,
                                                std::string_view key,
                                                EpochNumber read_epoch) {
  std::lock_guard<std::mutex> lk(purge_history_mtx_);
  if (read_epoch < purge_history_horizon_) {
    return AbsentReadCheck::HistoryExpired;
  }
  auto it = purge_history_.find(PurgeHistoryKey(index_identity, key));
  if (it != purge_history_.end() && it->second >= read_epoch) {
    return AbsentReadCheck::PurgedAfterRead;
  }
  return AbsentReadCheck::Ok;
}


}  // namespace Index
}  // namespace LineairDB
