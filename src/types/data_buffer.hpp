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

#ifndef LINEAIRDB_DATA_BUFFER_HPP
#define LINEAIRDB_DATA_BUFFER_HPP

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <new>
#include <string>
#include <vector>

#include "pax/pax_store.h"

namespace LineairDB {

namespace Pax {

/**
 * @brief Returns the process-wide count of rows that fell back from PAX cells.
 *
 * @details A non-zero count means the configured cell widths did not cover all
 * observed row payloads. Correctness is unaffected because those rows use heap
 * storage.
 */
inline std::atomic<uint64_t>& GlobalOverflowCount() {
  static std::atomic<uint64_t> count{0};
  return count;
}
}  // namespace Pax

/**
 * @brief Owns or references the row payload stored in a DataItem.
 *
 * @details Heap mode is the original layout: `value` owns a new[] byte array
 * of `size` bytes, with `capacity` tracking the allocation. It is used for
 * transaction-local copies, non-PAX tables, and rows that fell back from PAX
 * storage.
 *
 * PAX mode: the payload bytes live in a PaxGroup's column strips; this
 * buffer only references them. `value` carries a tagged pointer
 * (bit0 = PAX, bit1 = slot allocated): before the first install it points
 * to the table's PaxStore, afterwards to the owning PaxGroup with
 * `capacity` = slot index. `size` keeps its meaning (payload byte size,
 * 0 = tombstone/blank), so every liveness check (`size != 0`) works
 * unchanged in both modes.
 *
 * Mode transitions are one-way per row: index layers create blank items in
 * PAX mode for PAX tables; a row whose bytes don't fit the declared cell
 * widths permanently reverts to heap mode (correctness fallback).
 *
 * Copy semantics do the storage conversion implicitly: copying FROM a
 * PAX-mode buffer gathers the row into heap bytes (transaction-local
 * snapshots), assigning INTO a PAX-mode buffer scatters the bytes into the
 * strips (commit install under the row's TID lock).
 */
struct DataBuffer {
  static constexpr uintptr_t kPaxTag = 0x1;
  static constexpr uintptr_t kPaxAllocated = 0x2;
  static constexpr uintptr_t kPaxMask = kPaxTag | kPaxAllocated;

  std::byte* value;
  size_t size;
  size_t capacity;

  DataBuffer() : value(nullptr), size(0), capacity(0) {}
  ~DataBuffer() {
    if (value != nullptr && !is_pax()) delete[] value;
  }

  bool is_pax() const {
    return (reinterpret_cast<uintptr_t>(value) & kPaxTag) != 0;
  }
  bool pax_allocated() const {
    return (reinterpret_cast<uintptr_t>(value) & kPaxAllocated) != 0;
  }
  Pax::PaxGroup* pax_group() const {
    return reinterpret_cast<Pax::PaxGroup*>(
        reinterpret_cast<uintptr_t>(value) & ~kPaxMask);
  }
  Pax::PaxStore* pax_store() const {
    return reinterpret_cast<Pax::PaxStore*>(
        reinterpret_cast<uintptr_t>(value) & ~kPaxMask);
  }
  uint32_t pax_slot() const { return static_cast<uint32_t>(capacity); }

  /**
   * @brief Initializes a fresh blank item as a PAX-resident row reference.
   *
   * @param store Table store that will allocate the concrete row slot on the
   * first non-empty install.
   */
  void InitPaxBlank(Pax::PaxStore* store) {
    assert((reinterpret_cast<uintptr_t>(store) & kPaxMask) == 0);
    value = reinterpret_cast<std::byte*>(reinterpret_cast<uintptr_t>(store) |
                                         kPaxTag);
    size = 0;
    capacity = 0;
  }

  DataBuffer(DataBuffer&& other) noexcept
      : value(other.value), size(other.size), capacity(other.capacity) {
    other.value = nullptr;
    other.size = 0;
    other.capacity = 0;
  }

  DataBuffer& operator=(DataBuffer&& other) noexcept {
    if (this != &other) {
      if (value != nullptr && !is_pax()) delete[] value;
      value = other.value;
      size = other.size;
      capacity = other.capacity;
      other.value = nullptr;
      other.size = 0;
      other.capacity = 0;
    }
    return *this;
  }

  DataBuffer(const DataBuffer& other) : value(nullptr), size(0), capacity(0) {
    Reset(other);
  }

  DataBuffer& operator=(const DataBuffer& other) {
    if (this != &other) {
      Reset(other);
    }
    return *this;
  }

  // NOTE: capacity only grows; consider shrink-to-fit if large records cause bloat.
  void Reset(const std::byte* v, const size_t s) {
    if (is_pax()) {
      ResetPax(v, s);
      return;
    }
    if (v == nullptr || s == 0) {
      size = 0;
      return;
    }
    if (capacity < s) {
      delete[] value;
      value = new std::byte[s];
      capacity = s;
    }
    size = s;
    std::memcpy(value, v, s);
  }

  /**
   * @brief Resets this buffer to the row payload represented by `rhs`.
   */
  void Reset(const DataBuffer& rhs) {
    if (!rhs.is_pax()) {
      Reset(rhs.value, rhs.size);
      return;
    }
    if (rhs.size == 0) {
      Reset(nullptr, 0);
      return;
    }
    if (is_pax()) {
      // PAX-to-PAX copy gathers first, then scatters into this row's slot.
      thread_local std::vector<std::byte> tmp;
      tmp.resize(rhs.size);
      rhs.GatherInto(tmp.data());
      Reset(tmp.data(), rhs.size);
      return;
    }
    // Gather straight into our heap array for a transaction-local snapshot.
    if (capacity < rhs.size) {
      delete[] value;
      value = new std::byte[rhs.size];
      capacity = rhs.size;
    }
    size = rhs.GatherInto(value);
  }
  void Reset(const std::string& rhs) {
    Reset(reinterpret_cast<const std::byte*>(rhs.data()), rhs.size());
  }
  bool IsEmpty() const { return size == 0; }

  /**
   * @brief Reconstructs this row's payload bytes from PAX strips into `dst`.
   *
   * @param dst Destination buffer with room for `size` bytes.
   * @return Number of bytes written.
   */
  size_t GatherInto(std::byte* dst) const {
    assert(is_pax());
    assert(pax_allocated());
    assert(size > 0);
    return pax_group()->GatherRow(pax_slot(), dst, size);
  }

  std::string toString() const {
    if (!is_pax()) return std::string(reinterpret_cast<char*>(value), size);
    if (size == 0) return {};
    std::string out;
    out.resize(size);
    GatherInto(reinterpret_cast<std::byte*>(out.data()));
    return out;
  }

 private:
  /**
   * @brief Installs payload bytes into this PAX-mode buffer.
   *
   * @details The caller holds the row's TID lock. If the row does not fit its
   * declared cell widths, this buffer permanently switches to heap storage.
   */
  void ResetPax(const std::byte* v, const size_t s) {
    if (v == nullptr || s == 0) {
      size = 0;  // tombstone; keep the slot for the (possible) re-insert
      return;
    }
    if (!pax_allocated()) {
      auto* store = pax_store();
      auto [group, slot] = store->AllocateSlot();
      if (group == nullptr) {  // Table full: permanent heap fallback.
        Pax::GlobalOverflowCount().fetch_add(1, std::memory_order_relaxed);
        value = nullptr;
        capacity = 0;
        Reset(v, s);
        return;
      }
      assert((reinterpret_cast<uintptr_t>(group) & kPaxMask) == 0);
      value = reinterpret_cast<std::byte*>(reinterpret_cast<uintptr_t>(group) |
                                           kPaxTag | kPaxAllocated);
      capacity = slot;
    }
    if (pax_group()->ScatterRow(pax_slot(), v, s)) {
      size = s;
      return;
    }
    // Row does not fit (width overflow / shape mismatch): permanent heap
    // fallback for this row. The abandoned slot stays invisible (its cells
    // are only reachable through this buffer, which now points to heap).
    Pax::GlobalOverflowCount().fetch_add(1, std::memory_order_relaxed);
    value = nullptr;
    capacity = 0;
    Reset(v, s);
  }
};
}  // namespace LineairDB
#endif /* LINEAIRDB_DATA_BUFFER_HPP */
