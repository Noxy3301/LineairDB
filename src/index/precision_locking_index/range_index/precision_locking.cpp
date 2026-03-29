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

#include "precision_locking.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <functional>
#include <string_view>
#include <thread>

#include "transaction_impl.h"
#include "types/data_item.hpp"
#include "types/definitions.h"

namespace LineairDB {
namespace Index {

PrecisionLockingIndex::PrecisionLockingIndex(LineairDB::EpochFramework& e)
    : epoch_manager_ref_(e), manager_stop_flag_(false), manager_([&]() {
        while (manager_stop_flag_.load() != true) {
          epoch_manager_ref_.Sync();
          const auto global = epoch_manager_ref_.GetGlobalEpoch();
          const auto stable_epoch = global - 2;

          // Clear predicate list
          {
            std::lock_guard<std::shared_mutex> p_guard(predicate_lock_);
            auto it = predicate_list_.begin();
            if (it != predicate_list_.end() && it->first <= stable_epoch) {
              const auto beg = it;
              while (it != predicate_list_.end() &&
                     it->first <= stable_epoch) {
                it++;
              }
              predicate_list_.erase(beg, it);
            }
          }
          // Clear insert_or_delete_keys and update container.
          // 3-phase staging (copy -> apply -> erase): entries must remain
          // in insert_or_delete_key_set_ until container_ is updated,
          // otherwise Scan could miss a conflict and read stale data (phantom anomaly).
          std::vector<InsertOrDeleteEvent> ready;
          {
            // Phase 1: copy events (entries stay in map for Scan visibility)
            std::shared_lock<std::shared_mutex> u_guard(update_lock_);
            auto end_it = insert_or_delete_key_set_.upper_bound(stable_epoch);
            for (auto it = insert_or_delete_key_set_.begin(); it != end_it; ++it) {
              for (const auto& event : it->second) {
                ready.push_back(event);
              }
            }
          }
          if (!ready.empty()) {
            // Phase 2: apply to container.
            // Before deleting the set of insert_or_delete_keys, we update
            // the index container to apply such outdated (already
            // committed) insertions and deletions.
            {
              std::lock_guard<std::shared_mutex> c_guard(container_lock_);
              for (const auto& event : ready) {
                container_[event.key].is_deleted = event.is_delete_event;
              }
            }
            // Phase 3: erase from map
            {
              std::lock_guard<std::shared_mutex> u_guard(update_lock_);
              auto end_it = insert_or_delete_key_set_.upper_bound(stable_epoch);
              insert_or_delete_key_set_.erase(insert_or_delete_key_set_.begin(), end_it);
            }
          }
          last_processed_epoch_.store(stable_epoch,
                                      std::memory_order_release);
          // Sleep to avoid busy-wait; GC only needs to run once per epoch
          // FIXME: replace sleep loop with condition_variable notified on
          //        epoch advancement for responsive, adaptive GC timing.
          std::this_thread::sleep_for(std::chrono::milliseconds(40));
        }
      }){};

PrecisionLockingIndex::~PrecisionLockingIndex() {
  manager_stop_flag_.store(true);
  manager_.join();
};

std::optional<size_t> PrecisionLockingIndex::Scan(
    const std::string_view b, const std::optional<std::string_view> e,
    std::function<bool(std::string_view)> operation) {
  size_t hit = 0;
  const auto begin = std::string(b);
  auto end = begin;
  if (e.has_value()) {
    end = std::string(e.value());
    if (end < begin) return std::nullopt;
  }

  const auto epoch = epoch_manager_ref_.GetMyThreadLocalEpoch();
  void* ctx = GetCurrentTransactionContext();

  // Register-then-Check: register predicate FIRST, then check for conflicts.
  {
    std::lock_guard<std::shared_mutex> p_guard(predicate_lock_);
    predicate_list_[epoch].emplace_back(b, e);
    predicate_list_[epoch].back().tx_context = ctx;
  }

  // seq_cst fence: ensure our predicate registration is visible to all threads
  // before we check insert_or_delete_key_set_ (protected by a different mutex).
  std::atomic_thread_fence(std::memory_order_seq_cst);

  // Check for conflicting inserts (release update_lock_ before rollback)
  bool conflict = false;
  {
    std::shared_lock<std::shared_mutex> u_guard(update_lock_);
    conflict = IsOverlapWithInsertOrDelete(b, e);
  }

  if (conflict) {
    // Rollback: remove the predicate we just registered (no other lock held)
    std::lock_guard<std::shared_mutex> p_guard(predicate_lock_);
    auto& vec = predicate_list_[epoch];
    for (auto it = vec.rbegin(); it != vec.rend(); ++it) {
      if (it->tx_context == ctx && it->begin == std::string(b) && it->end == e) {
        vec.erase(std::next(it).base());
        break;
      }
    }
    return std::nullopt;
  }

  // Container traversal
  {
    std::shared_lock<std::shared_mutex> c_guard(container_lock_);
    auto it = container_.lower_bound(begin);
    auto it_end = container_.end();
    if (e.has_value()) {
      it_end = container_.upper_bound(end);
    }
    for (; it != it_end; it++) {
      if (it->second.is_deleted) continue;
      hit++;
      auto cancel = operation(it->first);
      if (cancel) break;
    }
  }

  return hit;
};

std::optional<size_t> PrecisionLockingIndex::ScanReverse(
    const std::string_view b, const std::optional<std::string_view> e,
    std::function<bool(std::string_view)> operation) {
  size_t hit = 0;
  const auto begin = std::string(b);
  auto end = begin;
  if (e.has_value()) {
    end = std::string(e.value());
    if (end < begin) return std::nullopt;
  }

  const auto epoch = epoch_manager_ref_.GetMyThreadLocalEpoch();
  void* ctx = GetCurrentTransactionContext();

  // Register-then-Check: register predicate FIRST, then check for conflicts.
  {
    std::lock_guard<std::shared_mutex> p_guard(predicate_lock_);
    predicate_list_[epoch].emplace_back(b, e);
    predicate_list_[epoch].back().tx_context = ctx;
  }

  // seq_cst fence: ensure our predicate registration is visible to all threads
  // before we check insert_or_delete_key_set_ (protected by a different mutex).
  std::atomic_thread_fence(std::memory_order_seq_cst);

  // Check for conflicting inserts (release update_lock_ before rollback)
  bool conflict = false;
  {
    std::shared_lock<std::shared_mutex> u_guard(update_lock_);
    conflict = IsOverlapWithInsertOrDelete(b, e);
  }

  if (conflict) {
    // Rollback: remove the predicate we just registered (no other lock held)
    std::lock_guard<std::shared_mutex> p_guard(predicate_lock_);
    auto& vec = predicate_list_[epoch];
    for (auto it = vec.rbegin(); it != vec.rend(); ++it) {
      if (it->tx_context == ctx && it->begin == std::string(b) && it->end == e) {
        vec.erase(std::next(it).base());
        break;
      }
    }
    return std::nullopt;
  }

  // Container traversal
  {
    std::shared_lock<std::shared_mutex> c_guard(container_lock_);
    auto it = container_.end();
    auto it_end = container_.lower_bound(begin);
    if (e.has_value()) {
      it = container_.upper_bound(end);
    }
    for (; it != it_end;) {
      --it;
      if (it->second.is_deleted) continue;
      hit++;
      auto cancel = operation(it->first);
      if (cancel) break;
    }
  }

  return hit;
};

bool PrecisionLockingIndex::Insert(const std::string_view key) {
  const auto epoch = epoch_manager_ref_.GetMyThreadLocalEpoch();
  void* ctx = GetCurrentTransactionContext();

  // Register-then-Check: register insert FIRST, then check predicates.
  {
    std::lock_guard<std::shared_mutex> u_guard(update_lock_);
    insert_or_delete_key_set_[epoch].emplace_back(key, false);
    insert_or_delete_key_set_[epoch].back().tx_context = ctx;
  }

  std::atomic_thread_fence(std::memory_order_seq_cst);

  // Check predicates (release predicate_lock_ before rollback)
  bool conflict = false;
  {
    std::shared_lock<std::shared_mutex> p_guard(predicate_lock_);
    conflict = IsInPredicateSet(key);
  }

  if (conflict) {
    // Rollback: remove the entry we just added (no other lock held)
    std::lock_guard<std::shared_mutex> u_guard(update_lock_);
    auto& vec = insert_or_delete_key_set_[epoch];
    for (auto it = vec.rbegin(); it != vec.rend(); ++it) {
      if (it->tx_context == ctx && it->key == key && !it->is_delete_event) {
        vec.erase(std::next(it).base());
        break;
      }
    }
    return false;
  }

  return true;
};

void PrecisionLockingIndex::ForceInsert(const std::string_view key) {
  const auto epoch = epoch_manager_ref_.GetMyThreadLocalEpoch();
  std::lock_guard<std::shared_mutex> u_guard(update_lock_);
  insert_or_delete_key_set_[epoch].emplace_back(key, false);
  insert_or_delete_key_set_[epoch].back().tx_context =
      GetCurrentTransactionContext();
}

bool PrecisionLockingIndex::Delete(const std::string_view key) {
  const auto epoch = epoch_manager_ref_.GetMyThreadLocalEpoch();
  void* ctx = GetCurrentTransactionContext();

  // Register-then-Check: register delete FIRST, then check predicates.
  {
    std::lock_guard<std::shared_mutex> u_guard(update_lock_);
    insert_or_delete_key_set_[epoch].emplace_back(key, true);
    insert_or_delete_key_set_[epoch].back().tx_context = ctx;
  }

  std::atomic_thread_fence(std::memory_order_seq_cst);

  // Check predicates (release predicate_lock_ before rollback)
  bool conflict = false;
  {
    std::shared_lock<std::shared_mutex> p_guard(predicate_lock_);
    conflict = IsInPredicateSet(key);
  }

  if (conflict) {
    // Rollback: remove the entry we just added (no other lock held)
    std::lock_guard<std::shared_mutex> u_guard(update_lock_);
    auto& vec = insert_or_delete_key_set_[epoch];
    for (auto it = vec.rbegin(); it != vec.rend(); ++it) {
      if (it->tx_context == ctx && it->key == key && it->is_delete_event) {
        vec.erase(std::next(it).base());
        break;
      }
    }
    return false;
  }

  return true;
};

bool PrecisionLockingIndex::Contains(const std::string_view key) {
  std::shared_lock<std::shared_mutex> c_guard(container_lock_);
  auto it = container_.find(std::string(key));
  if (it == container_.end()) return false;
  return !it->second.is_deleted;
}

bool PrecisionLockingIndex::IsInPredicateSet(const std::string_view key) {
  void* current_tx = GetCurrentTransactionContext();
  for (auto it = predicate_list_.begin(); it != predicate_list_.end(); it++) {
    for (const auto& predicate : it->second) {
      // Skip predicates from the same transaction (self-conflict)
      if (current_tx != nullptr && predicate.tx_context == current_tx) {
        continue;
      }
      const bool is_after_begin = predicate.begin <= key;
      const bool is_before_end =
          predicate.end.has_value() ? key <= predicate.end.value() : true;
      if (is_after_begin && is_before_end) return true;
    }
  }
  return false;
}

bool PrecisionLockingIndex::IsOverlapWithInsertOrDelete(
    const std::string_view begin, const std::optional<std::string_view> end) {
  void* current_tx = GetCurrentTransactionContext();
  for (auto it = insert_or_delete_key_set_.begin();
       it != insert_or_delete_key_set_.end(); it++) {
    for (const auto& event : it->second) {
      // Skip events from the same transaction (self-conflict)
      if (current_tx != nullptr && event.tx_context == current_tx) {
        continue;
      }
      const bool is_after_begin = begin <= event.key;
      const bool is_before_end =
          end.has_value() ? event.key <= end.value() : true;
      if (is_after_begin && is_before_end) {
        return true;
      }
    }
  }
  return false;
}

void PrecisionLockingIndex::WaitForIndexIsLinearizable() {
  // Wait until the manager thread processes all pending insert/delete events
  // and updates the container. This ensures that all index updates are visible.
  const auto target_epoch = epoch_manager_ref_.GetGlobalEpoch();
  const auto stable_epoch_target =
      target_epoch - 2;  // It assumes EpochManager#Sync()

  while (last_processed_epoch_.load(std::memory_order_acquire) <
         stable_epoch_target) {
    std::this_thread::yield();
  }
}

}  // namespace Index
}  // namespace LineairDB
