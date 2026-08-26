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

#ifndef LINEAIRDB_SNAPSHOT_HPP
#define LINEAIRDB_SNAPSHOT_HPP

#include <string>
#include <string_view>
#include <vector>

#include "data_item.hpp"
#include "definitions.h"
#include "index/secondary_index_type.h"

namespace LineairDB {
namespace Index {
class ConcurrentTable;
class SecondaryIndex;
}  // namespace Index

struct Snapshot {
  std::string key;
  DataItem data_item_copy;
  DataItem* index_cache;
  bool is_read_modify_write;
  std::string table_name;
  std::string index_name;
  Index::SecondaryIndexType index_type;
  // Owning table / SI of `key`. Populated at Snapshot construction time so
  // the OCC backend can structurally erase the entry from the underlying
  // index after a committed delete without re-resolving by table_name.
  // For primary-index writes pi_ref is non-null; for SI writes
  // si_ref is non-null. PL backends ignore both (PL doesn't expose
  // Purge).
  Index::ConcurrentTable* pi_ref = nullptr;
  Index::SecondaryIndex* si_ref = nullptr;
  // Set when this write claimed a key that held no row. Commit refuses it if
  // the claimed entry holds one by then.
  bool is_insert = false;
  struct SecondaryIndexDelta {
    std::string primary_key;
    SecondaryIndexOp op;
  };
  std::vector<SecondaryIndexDelta> secondary_index_deltas;

  Snapshot(const std::string_view k, const std::byte v[], const size_t s,
           DataItem* const i, std::string_view tn, std::string_view in,
           const TransactionId ver = 0,
           Index::SecondaryIndexType it = Index::SecondaryIndexType())
      : key(k),
        index_cache(i),
        is_read_modify_write(false),
        table_name(tn),
        index_name(in),
        index_type(it) {
    if (v != nullptr) data_item_copy.Reset(v, s, ver);
  }
  Snapshot(const Snapshot&) = default;
  Snapshot& operator=(const Snapshot&) = default;
  // Declaring copy ctor/assign above suppresses implicit move generation;
  // emplace_back(std::move(snapshot)) in transaction_impl.cpp would otherwise
  // silently fall back to the deep copy path below
  Snapshot(Snapshot&&) = default;
  Snapshot& operator=(Snapshot&&) = default;

  void RecordSecondaryIndexDelta(const std::string_view primary_key,
                                 SecondaryIndexOp op) {
    for (auto& delta : secondary_index_deltas) {
      if (delta.primary_key == primary_key) {
        delta.op = op;
        return;
      }
    }
    secondary_index_deltas.push_back({std::string(primary_key), op});
  }

  static bool Compare(const Snapshot& left, const Snapshot& right) {
    if (left.table_name != right.table_name) {
      return left.table_name < right.table_name;
    }
    if (left.index_name != right.index_name) {
      return left.index_name < right.index_name;
    }
    return left.key < right.key;
  }
};

using ReadSetType = std::vector<Snapshot>;
using WriteSetType = std::vector<Snapshot>;

}  // namespace LineairDB

#endif /* LINEAIRDB_SNAPSHOT_HPP */
