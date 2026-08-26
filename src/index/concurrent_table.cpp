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

#include "concurrent_table.h"

#include <functional>

#include "index/index_factory.hpp"
#include "lineairdb/config.h"
#include "types/data_item.hpp"
#include "types/definitions.h"

namespace LineairDB {
namespace Index {

ConcurrentTable::ConcurrentTable(EpochFramework& epoch_framework, Config config,
                                 WriteSetType recovery_set)
    : index_(MakeIndex(config, epoch_framework)),
      epoch_manager_ref_(epoch_framework) {
  if (recovery_set.empty()) return;
  for (auto& entry : recovery_set) {
    index_->Put(entry.key, DataItem(*entry.index_cache));
  }
}

DataItem* ConcurrentTable::Get(const std::string_view key) {
  return index_->Get(key);
}

DataItem* ConcurrentTable::GetOrInsert(const std::string_view key,
                                        NodeVersionUpdate* out_update) {
  auto* item = index_->Get(key);
  if (item == nullptr) {
    index_->ForcePutBlankEntry(key, out_update);
    item = index_->Get(key);
    assert(item != nullptr);
  }
  return item;
}

bool ConcurrentTable::Insert(const std::string_view key,
                              NodeVersionUpdate* out_update) {
  return index_->Insert(key, out_update);
}

bool ConcurrentTable::EnsureVisibleForSecondaryWrite(
    const std::string_view key, NodeVersionUpdate* out_update) {
  return index_->EnsureVisibleForSecondaryWrite(key, out_update);
}

// return false if a corresponding entry already exists
bool ConcurrentTable::Put(const std::string_view key, DataItem&& rhs,
                          NodeVersionUpdate* out_update) {
  return index_->Put(key, std::forward<decltype(rhs)>(rhs), out_update);
}

void ConcurrentTable::ForEach(
    std::function<bool(std::string_view, DataItem&)> f) {
  index_->ForEach(f);
};

std::optional<size_t> ConcurrentTable::Scan(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view)> operation,
    std::vector<NodeVersionEntry>* out_versions) {
  return index_->Scan(begin, end, operation, out_versions);
};

std::optional<size_t> ConcurrentTable::Scan(
    const std::string_view begin, const std::string_view end,
    std::function<bool(std::string_view, DataItem&)> operation,
    std::vector<NodeVersionEntry>* out_versions) {
  return index_->Scan(begin, end, operation, out_versions);
};

std::optional<size_t> ConcurrentTable::ScanReverse(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view)> operation,
    std::vector<NodeVersionEntry>* out_versions) {
  return index_->ScanReverse(begin, end, operation, out_versions);
};

std::optional<size_t> ConcurrentTable::ScanReverse(
    const std::string_view begin, const std::string_view end,
    std::function<bool(std::string_view, DataItem&)> operation,
    std::vector<NodeVersionEntry>* out_versions) {
  return index_->ScanReverse(begin, end, operation, out_versions);
};

bool ConcurrentTable::Delete(const std::string_view key) {
  return index_->Delete(key);
};

void ConcurrentTable::WaitForIndexIsLinearizable() {
  index_->WaitForIndexIsLinearizable();
}

bool ConcurrentTable::ValidatePhantoms(
    const std::vector<NodeVersionEntry>& entries) {
  return index_->ValidatePhantoms(entries);
}
}  // namespace Index
}  // namespace LineairDB
