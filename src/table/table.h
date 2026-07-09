#pragma once

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include <lineairdb/pax_store.h>

#include "index/concurrent_table.h"
#include "index/secondary_index.h"
#include "lineairdb/config.h"
#include "types/definitions.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {

class Table {
 public:
  Table(EpochFramework& epoch_framework, const Config& config,
        std::string_view table_name);

  bool CreateSecondaryIndex(
      const std::string_view index_name,
      [[maybe_unused]] const Index::SecondaryIndexType index_type) {
    std::unique_lock<std::shared_mutex> lk(table_lock_);
    if (secondary_indices_.count(std::string(index_name))) {
      return false;
    }
    auto new_index = std::make_unique<Index::SecondaryIndex>(
        epoch_framework_, config_, index_type);
    secondary_indices_[std::string(index_name)] = std::move(new_index);
    return true;
  }

  /**
   * @brief Installs PAX storage metadata for rows created after the call.
   *
   * @details The schema records per-field maximum cell widths. Existing rows
   * remain on the heap-backed DataBuffer layout; future rows are initialized
   * with the table's PaxStore. A table accepts only one PAX schema.
   *
   * @return true when the schema is installed for this table.
   * @return false when a schema has already been installed.
   */
  bool InstallPaxSchema(Pax::TableSchema schema) {
    std::unique_lock<std::shared_mutex> lk(table_lock_);
    if (pax_store_ != nullptr) return false;
    pax_store_ = std::make_unique<Pax::PaxStore>(std::move(schema));
    primary_index_.SetPaxStore(pax_store_.get());
    return true;
  }

  /**
   * @brief Returns the table's PAX store, or nullptr when PAX is disabled.
   */
  Pax::PaxStore* GetPaxStore() const { return pax_store_.get(); }

  bool Delete(const std::string_view key) { return primary_index_.Delete(key); }

  const std::string& GetTableName() const;

  Index::ConcurrentTable& GetPrimaryIndex();
  void WaitForIndexIsLinearizable();

  Index::SecondaryIndex* GetSecondaryIndex(const std::string_view index_name);

  size_t GetSecondaryIndexCount() const {
    std::shared_lock<std::shared_mutex> lk(table_lock_);
    return secondary_indices_.size();
  }

  template <typename Func>
  void ForEachSecondaryIndex(Func&& f) {
    std::shared_lock<std::shared_mutex> lk(table_lock_);
    for (auto& [index_name, index_ptr] : secondary_indices_) {
      f(index_name, *index_ptr);
    }
  }

  bool GetOrCreateSecondaryIndex(const std::string_view index_name,
                                 const Index::SecondaryIndexType index_type,
                                 Index::SecondaryIndex** out_index) {
    std::unique_lock<std::shared_mutex> lk(table_lock_);
    auto it = secondary_indices_.find(std::string(index_name));
    if (it != secondary_indices_.end()) {
      *out_index = it->second.get();
      return false;
    }
    auto new_index = std::make_unique<Index::SecondaryIndex>(
        epoch_framework_, config_, index_type);
    *out_index = new_index.get();
    secondary_indices_[std::string(index_name)] = std::move(new_index);
    return true;
  }

 private:
  EpochFramework& epoch_framework_;
  Config config_;
  Index::ConcurrentTable primary_index_;
  std::unique_ptr<Pax::PaxStore> pax_store_;
  mutable std::shared_mutex table_lock_;
  std::unordered_map<std::string, std::unique_ptr<Index::SecondaryIndex>>
      secondary_indices_;
  std::string table_name_;
};
}  // namespace LineairDB
