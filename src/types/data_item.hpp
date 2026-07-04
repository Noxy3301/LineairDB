/*
 *   Copyright (c) 2020 Nippon Telegraph and Telephone Corporation
 *   All rights reserved.

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

#ifndef LINEAIRDB_DATA_ITEM_HPP
#define LINEAIRDB_DATA_ITEM_HPP

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <xmmintrin.h>
#include <memory>
#include <msgpack.hpp>
#include <string>
#include <type_traits>
#include <vector>

#include "concurrency_control/pivot_object.hpp"
#include "data_buffer.hpp"
#include "lock/impl/readers_writers_lock.hpp"
#include "packed_primary_keys.hpp"
#include "types/transaction_id.hpp"
#include "util/logger.hpp"

namespace LineairDB {

struct DataItem {
  std::atomic<TransactionId> transaction_id;
  DataBuffer buffer;
  std::shared_ptr<const PackedPrimaryKeys> primary_keys_;

#ifdef LINEAIRDB_WITH_2PL_CHECKPOINT_METADATA
  std::vector<std::string> checkpoint_primary_keys;
  bool checkpoint_primary_keys_captured = false;
  /* std::unique_ptr<std::vector<DataBuffer>> sec_idx_buffers; */
  DataBuffer checkpoint_buffer;                     // a.k.a. stable version
#endif
#ifdef LINEAIRDB_WITH_NWR
  std::atomic<NWRPivotObject> pivot_object;         // for NWR
#else
  // Compile NWR/2PL sources without carrying one pivot object per record.
  // Startup rejects NWR when this process-wide dummy would be used.
  static inline std::atomic<NWRPivotObject> pivot_object{};
#endif
#ifdef LINEAIRDB_WITH_2PL_CHECKPOINT_METADATA
  Lock::ReadersWritersLockBO readers_writers_lock;  // for 2PL
#else
  // Slim layout for plain Silo with checkpointing disabled.
  // Startup rejects configs that would use these process-wide dummies.
  static inline std::vector<std::string> checkpoint_primary_keys{};
  static inline bool checkpoint_primary_keys_captured = false;
  static inline DataBuffer checkpoint_buffer{};
  static inline Lock::ReadersWritersLockBO readers_writers_lock{};
#endif

  // Direct byte access is invalid for PAX-resident rows (no contiguous
  // bytes); those callers must go through DataBuffer::GatherInto / copies.
  std::byte* value() {
    assert(!buffer.is_pax());
    return &buffer.value[0];
  }
  const std::byte* value() const {
    assert(!buffer.is_pax());
    return &buffer.value[0];
  }
  size_t size() const { return buffer.size; }
  bool IsInitialized() const {
    if (buffer.size != 0) return true;
    const auto primary_keys = std::atomic_load(&primary_keys_);
    return primary_keys && primary_keys->count != 0;
  }
  bool IsPrimaryInitialized() const { return buffer.size != 0; }

 private:
#ifndef LINEAIRDB_WITH_2PL_CHECKPOINT_METADATA
  [[noreturn]] static void AbortFullDataItemLayoutRequired(
      const char* feature) {
    SPDLOG_ERROR(
        "{} requires the full DataItem layout. Rebuild with "
        "-DLINEAIRDB_WITH_2PL_CHECKPOINT_METADATA.",
        feature);
    std::abort();
  }
#endif

 public:
  PackedPrimaryKeysView primary_keys_view() const {
    return PackedPrimaryKeysView(primary_keys_);
  }

  std::vector<std::string> primary_keys_vector() const {
    std::vector<std::string> keys;
    const auto view = primary_keys_view();
    keys.reserve(view.size());
    for (std::string_view key : view) {
      keys.emplace_back(key.data(), key.size());
    }
    return keys;
  }

  void SetPrimaryKeys(const std::vector<std::string>& primary_keys) {
    assert(IsSortedDeduped(primary_keys));
    auto packed = PackedPrimaryKeys::FromSortedDeduped(primary_keys);
    std::atomic_store(&primary_keys_, std::move(packed));
  }
  void SetPrimaryKeys(std::vector<std::string>&& primary_keys) {
    assert(IsSortedDeduped(primary_keys));
    auto packed = PackedPrimaryKeys::FromSortedDeduped(primary_keys);
    std::atomic_store(&primary_keys_, std::move(packed));
  }

  DataItem()
      : transaction_id(0)
#ifdef LINEAIRDB_WITH_NWR
        ,
        pivot_object(NWRPivotObject())
#endif
  {}
  DataItem(const DataItem& rhs)
      : transaction_id(rhs.transaction_id.load()),
        primary_keys_(std::atomic_load(&rhs.primary_keys_))
#ifdef LINEAIRDB_WITH_NWR
        ,
        pivot_object(NWRPivotObject())
#endif
  {
    buffer.Reset(rhs.buffer);
    /* if (rhs.sec_idx_buffers) {
      sec_idx_buffers =
          std::make_unique<std::vector<DataBuffer>>(*rhs.sec_idx_buffers);
    } */
  }

  DataItem& operator=(const DataItem& rhs) {
    transaction_id.store(rhs.transaction_id.load());
    buffer.Reset(rhs.buffer);

    /* if (rhs.sec_idx_buffers) {
      sec_idx_buffers =
          std::make_unique<std::vector<DataBuffer>>(*rhs.sec_idx_buffers);
    } else {
      sec_idx_buffers = nullptr;
    } */
    auto primary_keys = std::atomic_load(&rhs.primary_keys_);
    std::atomic_store(&primary_keys_, std::move(primary_keys));
    return *this;
  }

  DataItem(DataItem&& rhs) noexcept
      : transaction_id(rhs.transaction_id.load()),
        buffer(std::move(rhs.buffer)),
        primary_keys_(std::move(rhs.primary_keys_))
#ifdef LINEAIRDB_WITH_NWR
        ,
        pivot_object(rhs.pivot_object.load())
#endif
  {
#ifdef LINEAIRDB_WITH_2PL_CHECKPOINT_METADATA
    checkpoint_primary_keys = std::move(rhs.checkpoint_primary_keys);
    checkpoint_primary_keys_captured = rhs.checkpoint_primary_keys_captured;
    checkpoint_buffer = std::move(rhs.checkpoint_buffer);
#endif
  }

  DataItem& operator=(DataItem&& rhs) noexcept {
    transaction_id.store(rhs.transaction_id.load());
    buffer = std::move(rhs.buffer);
    std::atomic_store(&primary_keys_, std::move(rhs.primary_keys_));
#ifdef LINEAIRDB_WITH_NWR
    pivot_object.store(rhs.pivot_object.load());
#endif
#ifdef LINEAIRDB_WITH_2PL_CHECKPOINT_METADATA
    checkpoint_primary_keys = std::move(rhs.checkpoint_primary_keys);
    checkpoint_primary_keys_captured = rhs.checkpoint_primary_keys_captured;
    checkpoint_buffer = std::move(rhs.checkpoint_buffer);
#endif
    return *this;
  }

  void Reset(const std::byte* v, const size_t s, TransactionId tid = 0) {
    buffer.Reset(v, s);
    if (!tid.IsEmpty()) transaction_id.store(tid);
  }

  void AddSecondaryIndexValue(const std::byte* v, size_t s) {
    std::string_view new_key(reinterpret_cast<const char*>(v), s);
    auto current = std::atomic_load(&primary_keys_);
    auto next = PackedPrimaryKeys::Insert(current, new_key);
    if (next != current) {
      std::atomic_store(&primary_keys_, std::move(next));
    }
  }

  void RemoveSecondaryIndexValue(const std::byte* v, size_t s) {
    std::string_view target(reinterpret_cast<const char*>(v), s);
    auto current = std::atomic_load(&primary_keys_);
    auto next = PackedPrimaryKeys::Erase(current, target);
    if (next != current) {
      std::atomic_store(&primary_keys_, std::move(next));
    }
  }

  void CopyLiveVersionToStableVersion() {
#ifndef LINEAIRDB_WITH_2PL_CHECKPOINT_METADATA
    AbortFullDataItemLayoutRequired("Checkpoint stable version");
#else
    // There is an assumption that this thread can `exclusively` access this
    // data item.
    if (checkpoint_buffer.IsEmpty()) {
      checkpoint_buffer.Reset(buffer);
    }
    if (!checkpoint_primary_keys_captured) {
      checkpoint_primary_keys = primary_keys_vector();
      checkpoint_primary_keys_captured = true;
    }
#endif
  }

  bool HasCheckpointPrimaryKeys() const {
#ifndef LINEAIRDB_WITH_2PL_CHECKPOINT_METADATA
    AbortFullDataItemLayoutRequired("Checkpoint primary-key metadata");
#else
    return checkpoint_primary_keys_captured;
#endif
  }

  const std::vector<std::string>& GetCheckpointPrimaryKeys() const {
#ifndef LINEAIRDB_WITH_2PL_CHECKPOINT_METADATA
    AbortFullDataItemLayoutRequired("Checkpoint primary-key metadata");
#else
    return checkpoint_primary_keys;
#endif
  }

  void ClearCheckpointPrimaryKeys() {
#ifndef LINEAIRDB_WITH_2PL_CHECKPOINT_METADATA
    AbortFullDataItemLayoutRequired("Checkpoint primary-key metadata");
#else
    checkpoint_primary_keys.clear();
    checkpoint_primary_keys_captured = false;
#endif
  }

 private:
  static bool IsSortedDeduped(const std::vector<std::string>& keys) {
    return std::adjacent_find(keys.begin(), keys.end(),
                              [](const std::string& lhs,
                                 const std::string& rhs) {
                                return !(lhs < rhs);
                              }) == keys.end();
  }

 public:

  void ExclusiveLock() {
#ifndef LINEAIRDB_WITH_2PL_CHECKPOINT_METADATA
    AbortFullDataItemLayoutRequired("Checkpoint/2PL exclusive lock");
#endif
    // Acquire exclusive locking for all protocols:

    {
      // for Silo, Silo+NWR. they uses transaction_id as the lock
      for (;;) {
        auto tid = transaction_id.load();
        if (tid.tid & 1llu) {
          _mm_pause();
          continue;
        }
        auto new_tid = tid;
        new_tid.tid += 1llu;
        if (transaction_id.compare_exchange_weak(tid, new_tid)) break;
      }
    }

    // for TwoPhaseLocking. it uses rw_lock.
    { GetRWLockRef().Lock(); }
  }

  void ExclusiveUnlock() {
#ifndef LINEAIRDB_WITH_2PL_CHECKPOINT_METADATA
    AbortFullDataItemLayoutRequired("Checkpoint/2PL exclusive lock");
#endif
    // Release exclusive locking for all protocols:

    // for Silo, Silo+NWR. they uses transaction_id as the lock
    {
      auto tid = transaction_id.load();
      tid.tid -= 1llu;
      transaction_id.store(tid);
    }
    // for TwoPhaseLocking. it uses rw_lock.
    { GetRWLockRef().UnLock(); }
  }

  decltype(readers_writers_lock)& GetRWLockRef() {
#ifndef LINEAIRDB_WITH_2PL_CHECKPOINT_METADATA
    AbortFullDataItemLayoutRequired("2PL row lock");
#else
    return readers_writers_lock;
#endif
  };
};

#if !defined(LINEAIRDB_WITH_2PL_CHECKPOINT_METADATA) && \
    !defined(LINEAIRDB_WITH_NWR)
static_assert(sizeof(DataItem) == 48,
              "DataItem must remain 48 bytes in the slim layout");
#endif
}  // namespace LineairDB
#endif /* LINEAIRDB_DATA_ITEM_HPP */
