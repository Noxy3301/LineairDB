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

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <new>

namespace LineairDB {

struct DataBuffer {
  std::byte* value;
  size_t size;
  size_t capacity;

  DataBuffer() : value(nullptr), size(0), capacity(0) {}
  ~DataBuffer() {
    if (value != nullptr) delete[] value;
  }

  DataBuffer(DataBuffer&& other) noexcept
      : value(other.value), size(other.size), capacity(other.capacity) {
    other.value = nullptr;
    other.size = 0;
    other.capacity = 0;
  }

  DataBuffer& operator=(DataBuffer&& other) noexcept {
    if (this != &other) {
      delete[] value;
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
    Reset(other.value, other.size);
  }

  DataBuffer& operator=(const DataBuffer& other) {
    if (this != &other) {
      Reset(other.value, other.size);
    }
    return *this;
  }

  // NOTE: capacity only grows; consider shrink-to-fit if large records cause bloat.
  void Reset(const std::byte* v, const size_t s) {
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
  void Reset(const DataBuffer& rhs) { Reset(rhs.value, rhs.size); }
  void Reset(const std::string& rhs) {
    Reset(reinterpret_cast<const std::byte*>(rhs.data()), rhs.size());
  }
  bool IsEmpty() const { return size == 0; }

  std::string toString() const {
    return std::string(reinterpret_cast<char*>(value), size);
  }
};
}  // namespace LineairDB
#endif /* LINEAIRDB_DATA_BUFFER_HPP */
