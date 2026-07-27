/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef LINEAIRDB_RECOVERY_FLUSH_TRACE_H
#define LINEAIRDB_RECOVERY_FLUSH_TRACE_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "types/definitions.h"

namespace LineairDB {
namespace Recovery {

/**
 * @brief Timing census of the write-ahead log's flush groups.
 *
 * @details The response time of a synchronous durable commit is a single
 * end-to-end number, and the quantities that compose it (how long a group waits
 * to be scheduled, how many epochs it carries, how long its write and its
 * fdatasync take, and how long a waiter takes to resume once the group is
 * published) are not separable from outside the process. This records the
 * boundaries of each phase and leaves the arithmetic to offline analysis.
 *
 * Enabled by setting LINEAIRDB_FLUSH_TRACE to a path prefix. The gate is read
 * once at construction, and every call site tests it before reading a clock.
 * All storage, including each thread's commit buffer, is reserved during
 * construction: no recording path allocates, locks, or writes to a file.
 *
 * The trace observes; it never decides. No recording call alters control flow,
 * and none is placed between the durable watermark's store and the
 * notification that releases waiters.
 *
 * @par Output contract
 * Each dump writes its data files under its own generation, so no dump ever
 * replaces a file another dump published. `<prefix>_meta.csv` is the manifest:
 * it names the generation and is replaced last, atomically, and only if every
 * data file closed without error. A reader takes the generation from the
 * manifest and opens `<prefix>_g<generation>_*.csv`; until the manifest changes
 * it keeps describing the previous complete dump.
 *
 * A dump guarantees one immutable prefix per stream, not one instant across
 * streams: the counts are taken one after another, so a commit row can refer to
 * a group published after the group count was read. An analysis that joins
 * across the files has to tolerate a few unmatched rows at its end.
 */
class FlushTrace {
 public:
  /**
   * @brief One flush group that reached publication.
   *
   * A group that fails produces no row. A group that carries nothing to write
   * does produce one, with `write_begin` through `sync_end` left at zero,
   * because the flusher still publishes the target it was given; an analysis
   * that reads write or sync durations has to drop those rows.
   *
   * `encode_end` to `io_begin` is time in the bounded stage queue. The interval
   * between `io_begin` and `write_begin` holds the capacity check, which extends
   * and initialises the log when it is outgrown; that work is deliberately
   * outside every named phase so that `write` means the group's own write and
   * nothing else.
   */
  struct GroupRow {
    uint64_t seq;
    EpochNumber durable_before;
    EpochNumber target;
    uint64_t encoded_bytes;
    uint32_t epoch_count;
    int64_t collect_begin;
    int64_t collect_end;
    int64_t encode_begin;
    int64_t encode_end;
    int64_t io_begin;
    int64_t write_begin;
    int64_t write_end;
    int64_t sync_begin;
    int64_t sync_end;
    int64_t publish_enter;
    int64_t publish_exit;
  };

  /**
   * @brief One sampled commit's wait for its epoch to become durable.
   *
   * `not_durable_at_enter` reports what the watermark said when the commit
   * reached the wait, which is not the same as having slept: publication can
   * land between that reading and the wait's own check.
   */
  struct CommitRow {
    uint32_t slot;
    uint64_t local_seq;
    EpochNumber required_epoch;
    int64_t wait_enter;
    int64_t wait_return;
    uint8_t not_durable_at_enter;
  };

  /** @brief One epoch handed to the flusher as closed. */
  struct CloseRow {
    EpochNumber closed;
    int64_t close_enter;
    int64_t close_exit;
  };

  static FlushTrace& Instance() {
    static FlushTrace instance;
    return instance;
  }

  bool Enabled() const { return enabled_; }

  /** @brief Nanoseconds on the steady clock. Only differences are meaningful. */
  static int64_t Now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  }

  // --- Group census. Begin is called by the preparation stage and Publish by
  // the I/O stage. The row itself crosses the bounded stage queue, so preparing
  // group N+1 cannot overwrite group N's in-flight timings. ---

  GroupRow GroupBegin(EpochNumber durable_before) {
    GroupRow row{};
    if (!enabled_) return row;
    row.seq            = next_seq_++;
    row.durable_before = durable_before;
    row.collect_begin  = Now();
    return row;
  }

  void GroupPublish(GroupRow row, EpochNumber target, int64_t enter,
                    int64_t exit) {
    if (!enabled_) return;
    row.target        = target;
    row.publish_enter = enter;
    row.publish_exit  = exit;
    // Storage is sized once and never grows, so a reader can take the count and
    // walk the rows below it while this thread writes above it.
    const uint64_t index = group_count_.load(std::memory_order_relaxed);
    if (index < kGroupCapacity) {
      groups_[index] = row;
      group_count_.store(index + 1, std::memory_order_release);
    } else {
      group_drops_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  // --- Epoch closure. Called by the epoch framework's writer thread only. ---

  void EpochClosed(EpochNumber closed, int64_t enter, int64_t exit) {
    if (!enabled_) return;
    const uint64_t index = close_count_.load(std::memory_order_relaxed);
    if (index < kCloseCapacity) {
      closes_[index] = CloseRow{closed, enter, exit};
      close_count_.store(index + 1, std::memory_order_release);
    } else {
      close_drops_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  // --- Commit census. Called by any committing thread. ---

  /**
   * @brief Whether this commit is in the sample.
   *
   * The decision is made before the commit learns whether it will wait, so that
   * a commit finding its epoch already durable is represented. Each thread
   * draws from its own generator, which keeps the decision off any shared cache
   * line.
   */
  bool SampleThisCommit() {
    if (!enabled_) return false;
    uint64_t& state = ThreadState();
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    return (state & (kSampleEvery - 1)) == 0;
  }

  void RecordCommit(EpochNumber required_epoch, int64_t enter, int64_t exit,
                    bool not_durable_at_enter) {
    if (!enabled_) return;
    const uint32_t slot_index = ThreadSlotIndex();
    // A thread that arrived after the slots ran out records nothing rather than
    // sharing another thread's buffer, which would be a data race.
    if (slot_index >= kMaxSlots) {
      unslotted_drops_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    Slot& slot           = slots_[slot_index];
    const uint64_t index = slot.count.load(std::memory_order_relaxed);
    if (index < kCommitCapacity) {
      slot.rows[index] =
          CommitRow{slot_index, slot.next_seq++, required_epoch, enter, exit,
                    static_cast<uint8_t>(not_durable_at_enter ? 1 : 0)};
      slot.count.store(index + 1, std::memory_order_release);
    } else {
      slot.drops.fetch_add(1, std::memory_order_relaxed);
    }
  }

  /**
   * @brief Write the census out.
   *
   * Reached either from the instance's destruction or from a dump request
   * raised by SIGUSR1. The server is stopped with SIGKILL, which runs no
   * destructor, so a measurement asks for the census while the process is still
   * alive. Calls are serialised, and every count is taken before any file is
   * opened, so one dump reports one cut of the counters even though the
   * recording threads keep running. Rows written after that cut belong to the
   * next dump.
   */
  void Dump();

  /** @brief Ask for the census from a signal handler. Stores a flag and returns. */
  static void RequestDump() {
    dump_requested_.store(true, std::memory_order_relaxed);
  }

 private:
  // One row per group. The flusher wakes once per closed epoch, so a 1 ms epoch
  // produces about a thousand rows a second for the whole life of the process,
  // which spans loading as well as the measured run. A million rows covers a
  // thousand seconds.
  static constexpr size_t kGroupCapacity = 1048576;
  static constexpr size_t kCloseCapacity = 1048576;
  // Connection threads come and go across loading and the measured run, and a
  // thread that finds no slot free records nothing. Every buffer is reserved at
  // construction, which keeps the first sampled commit off an allocator.
  static constexpr size_t kMaxSlots       = 1024;
  static constexpr size_t kCommitCapacity = 2048;
  // A power of two, so the sampling test is a mask rather than a division.
  static constexpr uint64_t kSampleEvery = 16;

  static_assert(std::atomic<bool>::is_always_lock_free,
                "the dump request is stored from a signal handler");

  struct Slot {
    std::vector<CommitRow> rows;
    std::atomic<uint64_t> count{0};
    std::atomic<uint64_t> drops{0};
    uint64_t next_seq{0};
  };

  FlushTrace() {
    const char* prefix = std::getenv("LINEAIRDB_FLUSH_TRACE");
    enabled_ = prefix != nullptr && prefix[0] != '\0';
    if (!enabled_) return;
    prefix_          = prefix;
    bool usable      = false;
    dump_generation_ = FirstFreeGeneration(prefix_, &usable);
    if (!usable) {
      enabled_ = false;
      return;
    }
    groups_.resize(kGroupCapacity);
    closes_.resize(kCloseCapacity);
    slots_.reset(new Slot[kMaxSlots]);
    for (size_t i = 0; i < kMaxSlots; ++i) slots_[i].rows.resize(kCommitCapacity);
    InstallDumpSignal();
    // The request arrives as a flag from a signal handler; a thread outside
    // every measured path is what turns it into files.
    dumper_ = std::thread([this] {
      while (!dumper_stop_.load(std::memory_order_relaxed)) {
        if (dump_requested_.exchange(false, std::memory_order_relaxed)) Dump();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    });
  }

  ~FlushTrace() {
    if (!enabled_) return;
    dumper_stop_.store(true, std::memory_order_relaxed);
    if (dumper_.joinable()) dumper_.join();
  }

  static void InstallDumpSignal();

  /**
   * @brief The lowest generation this prefix does not already hold.
   *
   * A generation names a set of files that no other dump may replace, and a
   * process that started counting from zero would replace the files an earlier
   * process published under the same prefix. The existing files are what decide
   * where to start, so the property survives a restart.
   *
   * Sets @p usable to false when no generation can be claimed with certainty,
   * which disables tracing: producing no census is the only outcome that keeps
   * the promise never to replace one.
   */
  static uint64_t FirstFreeGeneration(const std::string& prefix, bool* usable);

  uint64_t& ThreadState() {
    // Seeded from this thread's own storage, which is distinct per thread and
    // costs no slot. Zero marks "not yet seeded" and is a value xorshift cannot
    // produce, so the seed is forced away from it.
    static thread_local uint64_t state = 0;
    if (state == 0) {
      state = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&state)) *
              0x9e3779b97f4a7c15ull;
      if (state == 0) state = 0x9e3779b97f4a7c15ull;
    }
    return state;
  }

  /** @brief This thread's buffer index, or kMaxSlots once they are exhausted. */
  uint32_t ThreadSlotIndex() {
    static thread_local uint32_t index =
        next_slot_.fetch_add(1, std::memory_order_relaxed);
    return index < kMaxSlots ? index : static_cast<uint32_t>(kMaxSlots);
  }

  bool enabled_{false};
  std::string prefix_;

  // GroupBegin has one caller (the preparer), and GroupPublish has one caller
  // (the ordered I/O stage), so neither counter needs a contended increment.
  uint64_t next_seq_{0};
  std::vector<GroupRow> groups_;
  std::atomic<uint64_t> group_count_{0};
  std::atomic<uint64_t> group_drops_{0};

  std::vector<CloseRow> closes_;
  std::atomic<uint64_t> close_count_{0};
  std::atomic<uint64_t> close_drops_{0};

  std::atomic<uint32_t> next_slot_{0};
  std::atomic<uint64_t> unslotted_drops_{0};
  // Held by pointer because a slot owns atomics and therefore cannot be moved
  // into place by a growing container.
  std::unique_ptr<Slot[]> slots_;

  std::mutex dump_mutex_;
  uint64_t dump_generation_{0};
  std::thread dumper_;
  std::atomic<bool> dumper_stop_{false};
  static std::atomic<bool> dump_requested_;
};

}  // namespace Recovery
}  // namespace LineairDB

#endif /* LINEAIRDB_RECOVERY_FLUSH_TRACE_H */
