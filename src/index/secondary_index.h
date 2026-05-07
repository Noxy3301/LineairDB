#ifndef LINEAIRDB_SECONDARY_INDEX_H
#define LINEAIRDB_SECONDARY_INDEX_H

#include "index/index_base.h"
#include "index/index_factory.hpp"
#include "index/secondary_index_type.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

class SecondaryIndex {
 public:
  SecondaryIndex(EpochFramework& epoch_framework, Config config = Config(),
                 SecondaryIndexType index_type = SecondaryIndexType(),
                 [[maybe_unused]] WriteSetType recovery_set = WriteSetType())
      : index_type_(index_type),
        secondary_index_(MakeIndex(config, epoch_framework)) {}

  DataItem* Get(std::string_view key) { return secondary_index_->Get(key); }

  DataItem* GetOrInsert(std::string_view key,
                         NodeVersionUpdate* out_update = nullptr) {
    auto* item = secondary_index_->Get(key);
    if (item == nullptr) {
      secondary_index_->ForcePutBlankEntry(key, out_update);
      item = secondary_index_->Get(key);
      assert(item != nullptr);
    }
    return item;
  }

  DataItem* GetOrInsertForWrite(std::string_view key,
                                  NodeVersionUpdate* out_update = nullptr) {
    // OCC guards existing entries; new keys (and PL's point-present /
    // range-absent DELETED slots, where IsInitialized() is false) still need
    // EnsureVisibleForSecondaryWrite so the range index can run its phantom
    // detection.
    auto* item = secondary_index_->Get(key);
    if (item == nullptr || !item->IsInitialized()) {
      if (!secondary_index_->EnsureVisibleForSecondaryWrite(key, out_update))
        return nullptr;
      item = secondary_index_->Get(key);
      assert(item != nullptr);
    }
    return item;
  }

  std::optional<size_t> Scan(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> operation,
      std::vector<NodeVersionEntry>* out_versions = nullptr) {
    return secondary_index_->Scan(begin, end, operation, out_versions);
  }

  std::optional<size_t> ScanReverse(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> operation,
      std::vector<NodeVersionEntry>* out_versions = nullptr) {
    return secondary_index_->ScanReverse(begin, end, operation, out_versions);
  }

  bool ValidatePhantoms(const std::vector<NodeVersionEntry>& entries) {
    return secondary_index_->ValidatePhantoms(entries);
  }

  bool Delete(std::string_view key) {
    // Range-only deletion to hide the key from scans.
    // Point index does not support erase currently.
    // Implement as a method on HashTableWithPrecisionLockingIndex when
    // available.
    return secondary_index_->Delete(key);
  }

  void ForEach(std::function<bool(std::string_view, DataItem&)> f) {
    secondary_index_->ForEach(f);
  }

  bool Put(const std::string_view key, DataItem&& value,
           NodeVersionUpdate* out_update = nullptr) {
    return secondary_index_->Put(key, std::forward<DataItem>(value), out_update);
  }

  bool IsUnique() { return index_type_.IsUnique(); }

  SecondaryIndexType GetIndexType() const { return index_type_; }

  void WaitForIndexIsLinearizable() {
    secondary_index_->WaitForIndexIsLinearizable();
  }

 private:
  SecondaryIndexType index_type_;
  std::unique_ptr<IndexBase> secondary_index_;
};
}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_SECONDARY_INDEX_H */
