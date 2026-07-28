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

#ifndef LINEAIRDB_EPOCH_FRAMEWORK_H_
#define LINEAIRDB_EPOCH_FRAMEWORK_H_

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>

#include "util/thread_key_storage.h"

namespace LineairDB {

/**
 * @brief
 * Generic framework for thread-safely synchronizing objects.
 * It provides the concept of epoch, the monotonically increasing number which
 * is shared by all threads.
 * This number ensures the thread-safely object deletion.
 * When a thread see that an epoch number of an object is same with the return
 * value of #current_epoch, it means that the object may be accessed
 * simultaneously by the other threads and it is dangerous to do free/delete
 * into the object.
 * @see [Silo]: https://dl.acm.org/doi/10.1145/2517349.2522713
 * @see [FASTER]:
 * https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf
 */
class EpochFramework {
 public:
  static constexpr EpochNumber THREAD_OFFLINE = UINT32_MAX;

 public:
  EpochFramework(size_t epoch_duration_us = 40000)
      : start_(false),
        stop_(false),
        global_epoch_(1),
        epoch_writer_([=]() { EpochWriterJob(epoch_duration_us); }) {}
  EpochFramework(size_t epoch_duration_us,
                 std::function<void(EpochNumber)>&& pt)
      : start_(false),
        stop_(false),
        global_epoch_(1),
        publish_target_(pt),
        epoch_writer_([=]() { EpochWriterJob(epoch_duration_us); }) {}

  ~EpochFramework() { Stop(); }

  void SetGlobalEpoch(const EpochNumber epoch) { global_epoch_.store(epoch); }

  EpochNumber GetGlobalEpoch() const { return global_epoch_.load(); }

  /**
   * Returns the epoch this thread currently participates in, or
   * #THREAD_OFFLINE when it participates in none. The slot itself is never
   * exposed: every read and write of it happens inside this class so that the
   * publication order relied on by #GetSmallestEpoch stays under one
   * sequentially consistent total order.
   */
  EpochNumber GetMyThreadLocalEpoch() {
    std::atomic<EpochNumber>* my_epoch =
        tls_.Get<EpochNumber>([]() { return THREAD_OFFLINE; });
    return my_epoch->load(std::memory_order_seq_cst);
  }

  /**
   * Overwrites the calling thread's epoch with a replayed one. Valid only
   * before #Start(), where the epoch writer has not begun scanning slots and
   * the caller is the only participant; ordinary transactions must reach their
   * epoch through #MakeMeOnline so that the closure property below holds.
   */
  void SetMyThreadLocalEpochForRecovery(const EpochNumber epoch) {
    assert(!start_.load(std::memory_order_seq_cst));
    assert(epoch != THREAD_OFFLINE);
    std::atomic<EpochNumber>* my_epoch =
        tls_.Get<EpochNumber>([]() { return THREAD_OFFLINE; });
    assert(my_epoch->load(std::memory_order_seq_cst) != THREAD_OFFLINE);
    my_epoch->store(epoch, std::memory_order_seq_cst);
  }

  /**
   * Joins the current epoch and returns it.
   *
   * Publication is stabilized: the loop republishes until the global epoch
   * reloaded after the store still equals what was stored. This establishes
   * the closure property the durability path depends on -- once the global
   * epoch reaches U, no thread remains online in an epoch at or below U-2.
   * A single store cannot establish it: an epoch writer that read the slot as
   * #THREAD_OFFLINE just before the store may advance the global epoch twice
   * while the thread is online in the epoch it published.
   *
   * The caller must not begin its transaction, and in particular must not
   * enqueue log records, before this function returns.
   */
  EpochNumber MakeMeOnline() {
    std::atomic<EpochNumber>* my_epoch =
        tls_.Get<EpochNumber>([]() { return THREAD_OFFLINE; });
    assert(my_epoch->load(std::memory_order_seq_cst) == THREAD_OFFLINE);

    EpochNumber published = global_epoch_.load(std::memory_order_seq_cst);
    for (;;) {
      my_epoch->store(published, std::memory_order_seq_cst);
      const EpochNumber reloaded =
          global_epoch_.load(std::memory_order_seq_cst);
      if (reloaded == published) return published;
      // The writer advanced between the store and the reload; the slot
      // currently holds a stale epoch. Republish the observed value. Each
      // retry consumes one real advance, so the writer never spins here.
      published = reloaded;
    }
  }

  void MakeMeOffline() {
    std::atomic<EpochNumber>* my_epoch =
        tls_.Get<EpochNumber>([]() { return THREAD_OFFLINE; });
    assert(my_epoch->load(std::memory_order_seq_cst) != THREAD_OFFLINE);
    my_epoch->store(THREAD_OFFLINE, std::memory_order_seq_cst);
  }

  EpochNumber Sync() {
    assert(GetMyThreadLocalEpoch() == THREAD_OFFLINE);
    size_t reload_count = 0;
    for (;;) {
      auto current_epoch = global_epoch_.load();
      {
        std::unique_lock<std::mutex> lk(epoch_mtx_);
        epoch_cv_.wait(lk, [&] {
          return stop_.load() || (global_epoch_.load() != current_epoch);
        });
      }
      auto reload_epoch = global_epoch_.load();
      if (stop_.load()) return reload_epoch;
      reload_count++;

      // Note that each thread always belongs to either one of the two epochs,
      // the old one and the one that matches the global epoch.
      // The first while loop waits for all threads that are in the old epoch
      // when Sync() is called. The next while loop waits for all threads to
      // progress to the next epoch.
      if (reload_count == 2) return reload_epoch;
    }
  }

  // Margin below the uint32 wrap point. Forced advances and fences refuse
  // beyond it, and a read view's epoch lifetime is bounded well under the
  // margin; every read-view epoch comparison therefore stays inside one
  // wrap-free window where plain unsigned ordering is exact. The timer-driven
  // advance stops the process at the mark rather than wrapping: past the wrap,
  // the epoch no longer orders against the durability frontier, so a commit
  // could be acknowledged as durable against a comparison that has lost its
  // meaning.
  static constexpr EpochNumber kEpochHighWater = UINT32_MAX - (1u << 20);

  // Asks the epoch writer to run its advance check now instead of at the
  // next tick. The advance condition itself is unchanged. No-op at or
  // above the high-water mark.
  void RequestEpochAdvance() {
    if (global_epoch_.load() >= kEpochHighWater) return;
    {
      std::lock_guard<std::mutex> lk(epoch_mtx_);
      advance_requested_.store(true);
    }
    worker_cv_.notify_one();
  }

  // Blocks the OFFLINE caller until it observes global_epoch >= target.
  // Already-reached targets succeed even after Stop() or above the
  // high-water mark; otherwise false on timeout, Stop(), or a target
  // beyond the mark.
  bool WaitGlobalEpochAtLeast(EpochNumber target,
                              std::chrono::milliseconds timeout) {
    assert(GetMyThreadLocalEpoch() == THREAD_OFFLINE);
    if (global_epoch_.load() >= target) return true;
    if (target > kEpochHighWater) return false;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::unique_lock<std::mutex> lk(epoch_mtx_);
    for (;;) {
      if (global_epoch_.load() >= target) return true;
      if (stop_.load()) return false;
      advance_requested_.store(true);
      worker_cv_.notify_one();
      if (epoch_cv_.wait_until(lk, deadline) == std::cv_status::timeout) {
        return global_epoch_.load() >= target;
      }
    }
  }

  void Start() {
    {
      std::lock_guard<std::mutex> lk(epoch_mtx_);
      start_.store(true);
    }
    epoch_cv_.notify_all();
  }
  void Stop() {
    {
      std::lock_guard<std::mutex> lk(epoch_mtx_);
      stop_.store(true);
    }
    epoch_cv_.notify_all();
    worker_cv_.notify_all();
    if (epoch_writer_.joinable()) epoch_writer_.join();
  }

 public:
  uint32_t GetSmallestEpoch() {
    uint32_t min_epoch = THREAD_OFFLINE;
    tls_.ForEach([&](const std::atomic<EpochNumber>* local_epoch) {
      const EpochNumber e = local_epoch->load(std::memory_order_seq_cst);
      if (0 < e && e < min_epoch) {
        min_epoch = e;
      }
    });

    return min_epoch;
  }

  void EpochWriterJob(size_t epoch_duration_us) {
    const auto epoch_duration = std::chrono::microseconds(epoch_duration_us);
    {
      std::unique_lock<std::mutex> lk(epoch_mtx_);
      epoch_cv_.wait(lk, [&] { return start_.load(); });
    }

    for (;;) {
      bool forced_wake = false;
      if (stop_.load()) {
        // Post-stop the predicate below stays true; plain sleep keeps the
        // cadence while draining still-online threads
        std::this_thread::sleep_for(epoch_duration);
      } else {
        // Forced requests wake the writer early; the advance condition
        // below still gates
        std::unique_lock<std::mutex> lk(epoch_mtx_);
        forced_wake = worker_cv_.wait_for(
            lk, epoch_duration, [&] {
              return advance_requested_.load() || stop_.load();
            });
        advance_requested_.store(false);
      }
      EpochNumber min_epoch = GetSmallestEpoch();
      EpochNumber old_epoch = global_epoch_;
      if (forced_wake && !stop_.load() && old_epoch >= kEpochHighWater) {
        // A racing forced request must not advance past the high-water
        // margin; timer cadence continues
        continue;
      }
      if (min_epoch == THREAD_OFFLINE || min_epoch == old_epoch) {
        if (old_epoch >= kEpochHighWater) {
          // Stopping here is the conservative end: an epoch that wraps stops
          // ordering against the durability frontier, and refusing to advance
          // instead would stall every commit that waits for its epoch to close.
          fprintf(stderr,
                  "LineairDB: the global epoch reached the high-water mark "
                  "%u and cannot advance without wrapping\n",
                  kEpochHighWater);
          std::abort();
        }
        {
          // fetch_add is atomic, but we hold epoch_mtx_ here to
          // ensure Sync()'s cv.wait does not miss the subsequent
          // notify_all (prevents lost-wake race).
          std::lock_guard<std::mutex> lk(epoch_mtx_);
          global_epoch_.fetch_add(1);
        }
        EpochNumber updated = global_epoch_.load();
        epoch_cv_.notify_all();
        if (publish_target_) publish_target_(updated);
      }
      if (stop_.load() && min_epoch == THREAD_OFFLINE) break;
    }
  }

 private:
  std::atomic<bool> start_;
  std::atomic<bool> stop_;
  std::atomic<bool> advance_requested_{false};
  std::atomic<EpochNumber> global_epoch_;
  std::mutex epoch_mtx_;
  std::condition_variable epoch_cv_;
  std::condition_variable worker_cv_;
  const std::function<void(EpochNumber)> publish_target_;
  std::thread epoch_writer_;
  ThreadKeyStorage<std::atomic<EpochNumber>> tls_;
};

}  // namespace LineairDB
#endif
