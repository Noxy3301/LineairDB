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
  EpochFramework(size_t epoch_duration_ms = 40)
      : start_(false),
        stop_(false),
        global_epoch_(1),
        epoch_writer_([=]() { EpochWriterJob(epoch_duration_ms); }) {}
  EpochFramework(size_t epoch_duration_ms,
                 std::function<void(EpochNumber)>&& pt)
      : start_(false),
        stop_(false),
        global_epoch_(1),
        publish_target_(pt),
        epoch_writer_([=]() { EpochWriterJob(epoch_duration_ms); }) {}

  ~EpochFramework() { Stop(); }

  void SetGlobalEpoch(const EpochNumber epoch) { global_epoch_.store(epoch); }

  EpochNumber GetGlobalEpoch() const { return global_epoch_.load(); }

  /**
   * Returns the epoch this thread participates in, or #THREAD_OFFLINE for
   * none. The slot stays private so that every access to it shares one
   * sequentially consistent order with #GetSmallestEpoch's scan.
   */
  EpochNumber GetMyThreadLocalEpoch() {
    std::atomic<EpochNumber>* my_epoch =
        tls_.Get<EpochNumber>([]() { return THREAD_OFFLINE; });
    return my_epoch->load(std::memory_order_seq_cst);
  }

  /**
   * Overwrites this thread's epoch with a replayed one. Valid only before
   * #Start(), where the epoch writer has not begun scanning slots.
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
   * Joins the current epoch and returns it. The caller must not enqueue log
   * records, and must not take its commit epoch, before this returns.
   *
   * Once this has returned an epoch E, the global epoch cannot reach E+2 while
   * the slot still reads E: only one writer scan that missed the publication
   * can be outstanding, and the next one reads E. This assumes E stays clear
   * of wraparound (#kEpochHighWater); a wrapped counter can hand out 0, which
   * scans skip. A thread still inside the loop carries no such guarantee,
   * which is why the contract above exists.
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
      // A single store does not suffice: two consecutive writer scans can
      // read the slot before the store lands, and the advances they gate
      // leave this thread online two epochs behind. Republish until the
      // reload agrees.
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
  // wrap-free window where plain unsigned ordering is exact. The
  // timer-driven advance is not capped.
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

  void EpochWriterJob(size_t epoch_duration_ms) {
    const uint64_t epoch_duration = epoch_duration_ms * 1000 * 1000;
    {
      std::unique_lock<std::mutex> lk(epoch_mtx_);
      epoch_cv_.wait(lk, [&] { return start_.load(); });
    }

    for (;;) {
      bool forced_wake = false;
      if (stop_.load()) {
        // Post-stop the predicate below stays true; plain sleep keeps the
        // cadence while draining still-online threads
        std::this_thread::sleep_for(std::chrono::nanoseconds(epoch_duration));
      } else {
        // Forced requests wake the writer early; the advance condition
        // below still gates
        std::unique_lock<std::mutex> lk(epoch_mtx_);
        forced_wake = worker_cv_.wait_for(
            lk, std::chrono::nanoseconds(epoch_duration), [&] {
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
