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

#include "flush_trace.h"

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <sys/stat.h>

#include <cinttypes>
#include <functional>

namespace LineairDB {
namespace Recovery {

std::atomic<bool> FlushTrace::dump_requested_{false};

namespace {

void OnDumpSignal(int) { FlushTrace::RequestDump(); }

/**
 * @brief Write one file, and give it its final name only if nothing failed.
 *
 * A reader decides a file is complete by its name, so a partial write must not
 * reach that name. Every step is checked: a formatting error, a full
 * filesystem, or a failure inside fclose all leave the final name absent.
 */
bool WriteFile(const std::string& path,
               const std::function<bool(FILE*)>& emit) {
  const std::string temporary = path + ".partial";
  FILE* file = std::fopen(temporary.c_str(), "w");
  if (file == nullptr) {
    std::fprintf(stderr, "flush trace: cannot write %s\n", temporary.c_str());
    return false;
  }
  const bool emitted = emit(file);
  const bool flushed = std::fflush(file) == 0;
  const bool closed  = std::fclose(file) == 0;
  if (!emitted || !flushed || !closed) {
    std::fprintf(stderr, "flush trace: %s is incomplete and was discarded\n",
                 path.c_str());
    std::remove(temporary.c_str());
    return false;
  }
  if (std::rename(temporary.c_str(), path.c_str()) != 0) {
    std::fprintf(stderr, "flush trace: cannot name %s\n", path.c_str());
    std::remove(temporary.c_str());
    return false;
  }
  return true;
}

}  // namespace

void FlushTrace::InstallDumpSignal() {
  struct sigaction action = {};
  action.sa_handler       = OnDumpSignal;
  sigemptyset(&action.sa_mask);
  action.sa_flags = SA_RESTART;
  // The process gains a SIGUSR1 handler for as long as tracing is enabled, and
  // whatever disposition was in place is replaced.
  if (sigaction(SIGUSR1, &action, nullptr) != 0) {
    std::fprintf(stderr,
                 "flush trace: SIGUSR1 is not available; the census can only "
                 "be written by a clean shutdown\n");
  }
}

uint64_t FlushTrace::FirstFreeGeneration(const std::string& prefix,
                                         bool* usable) {
  // The groups file is written for every generation, so its absence marks the
  // first free one. Only a name that is absent counts as free: a name that
  // cannot be examined might already hold a published census, and replacing it
  // is what this exists to prevent.
  constexpr uint64_t kCeiling = 4096;
  for (uint64_t generation = 0; generation < kCeiling; ++generation) {
    const std::string path =
        prefix + "_g" + std::to_string(generation) + "_groups.csv";
    struct stat info {};
    if (::stat(path.c_str(), &info) == 0) continue;
    if (errno == ENOENT) {
      *usable = true;
      return generation;
    }
    std::fprintf(stderr,
                 "flush trace: %s cannot be examined (errno %d); tracing is "
                 "disabled rather than risk replacing a census\n",
                 path.c_str(), errno);
    *usable = false;
    return 0;
  }
  std::fprintf(stderr,
               "flush trace: %s already holds %" PRIu64
               " censuses; tracing is disabled rather than replace one\n",
               prefix.c_str(), kCeiling);
  *usable = false;
  return 0;
}

void FlushTrace::Dump() {
  if (!enabled_) return;
  std::lock_guard<std::mutex> serialise(dump_mutex_);

  // Each dump owns its generation, so it never replaces a file another dump
  // published and the manifest keeps describing the previous one until this dump
  // succeeds.
  const uint64_t generation = dump_generation_;
  const std::string stem =
      prefix_ + "_g" + std::to_string(generation);

  // Every count is taken before any file is opened. That makes each file an
  // immutable prefix of its stream; it does not make the four files one instant,
  // because the counts are read one after another.  Storage never grows, so rows
  // below a count are settled even while the threads that own them keep writing
  // above it.
  const uint64_t groups = group_count_.load(std::memory_order_acquire);
  const uint64_t closes = close_count_.load(std::memory_order_acquire);
  std::vector<uint64_t> commit_counts(kMaxSlots);
  uint64_t commit_rows = 0;
  for (size_t i = 0; i < kMaxSlots; ++i) {
    commit_counts[i] = slots_[i].count.load(std::memory_order_acquire);
    commit_rows += commit_counts[i];
  }
  uint64_t commit_drops = 0;
  for (size_t i = 0; i < kMaxSlots; ++i) {
    commit_drops += slots_[i].drops.load(std::memory_order_relaxed);
  }
  const uint64_t group_drops = group_drops_.load(std::memory_order_relaxed);
  const uint64_t close_drops = close_drops_.load(std::memory_order_relaxed);
  const uint64_t unslotted   = unslotted_drops_.load(std::memory_order_relaxed);
  const uint32_t claimed     = next_slot_.load(std::memory_order_relaxed);
  const uint32_t slots_used =
      claimed < kMaxSlots ? claimed : static_cast<uint32_t>(kMaxSlots);

  bool complete = true;

  complete &= WriteFile(stem + "_groups.csv", [&](FILE* file) {
    if (std::fprintf(file,
                     "seq,durable_before,target,encoded_bytes,epoch_count,"
                     "collect_begin,collect_end,encode_begin,encode_end,"
                     "io_begin,write_begin,write_end,sync_begin,sync_end,"
                     "publish_enter,publish_exit\n") < 0) {
      return false;
    }
    for (uint64_t i = 0; i < groups; ++i) {
      const GroupRow& row = groups_[i];
      if (std::fprintf(file,
                       "%" PRIu64 ",%u,%u,%" PRIu64 ",%u,%" PRId64 ",%" PRId64
                       ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64
                       ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64
                       ",%" PRId64 "\n",
                       row.seq, row.durable_before, row.target,
                       row.encoded_bytes, row.epoch_count, row.collect_begin,
                       row.collect_end, row.encode_begin, row.encode_end,
                       row.io_begin, row.write_begin, row.write_end,
                       row.sync_begin, row.sync_end, row.publish_enter,
                       row.publish_exit) < 0) {
        return false;
      }
    }
    return true;
  });

  complete &= WriteFile(stem + "_closes.csv", [&](FILE* file) {
    if (std::fprintf(file, "closed,close_enter,close_exit\n") < 0) return false;
    for (uint64_t i = 0; i < closes; ++i) {
      const CloseRow& row = closes_[i];
      if (std::fprintf(file, "%u,%" PRId64 ",%" PRId64 "\n", row.closed,
                       row.close_enter, row.close_exit) < 0) {
        return false;
      }
    }
    return true;
  });

  complete &= WriteFile(stem + "_commits.csv", [&](FILE* file) {
    if (std::fprintf(file,
                     "slot,local_seq,required_epoch,wait_enter,wait_return,"
                     "not_durable_at_enter\n") < 0) {
      return false;
    }
    for (size_t slot_index = 0; slot_index < kMaxSlots; ++slot_index) {
      const Slot& slot = slots_[slot_index];
      for (uint64_t i = 0; i < commit_counts[slot_index]; ++i) {
        const CommitRow& row = slot.rows[i];
        if (std::fprintf(file, "%u,%" PRIu64 ",%u,%" PRId64 ",%" PRId64 ",%u\n",
                         row.slot, row.local_seq, row.required_epoch,
                         row.wait_enter, row.wait_return,
                         row.not_durable_at_enter) < 0) {
          return false;
        }
      }
    }
    return true;
  });

  if (!complete) {
    // The manifest is what a reader follows; leaving it pointing at the previous
    // generation is how an incomplete census reports itself.
    std::fprintf(stderr,
                 "flush trace: generation %" PRIu64
                 " is incomplete and the manifest was left alone\n",
                 generation);
    return;
  }

  // A dropped row is a truncated census, not a missing sample: any analysis
  // that reads these files has to see the count rather than infer completeness.
  const bool published = WriteFile(prefix_ + "_meta.csv", [&](FILE* file) {
    if (std::fprintf(file,
                     "generation,groups,group_drops,closes,close_drops,commits,"
                     "commit_drops,unslotted_drops,slots_used,sample_every\n") <
        0) {
      return false;
    }
    return std::fprintf(file,
                        "%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64
                        ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64
                        ",%u,%" PRIu64 "\n",
                        generation, groups, group_drops, closes, close_drops,
                        commit_rows, commit_drops, unslotted, slots_used,
                        kSampleEvery) >= 0;
  });
  if (!published) return;
  ++dump_generation_;

  if (group_drops != 0 || close_drops != 0 || commit_drops != 0 ||
      unslotted != 0) {
    std::fprintf(stderr,
                 "flush trace: dropped %" PRIu64 " groups, %" PRIu64
                 " closes, %" PRIu64 " commits, %" PRIu64
                 " commits from threads with no slot\n",
                 group_drops, close_drops, commit_drops, unslotted);
  }
}

}  // namespace Recovery
}  // namespace LineairDB
