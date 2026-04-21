#ifndef LINEAIRDB_INDEX_IMPL_PL_INDEX_HPP
#define LINEAIRDB_INDEX_IMPL_PL_INDEX_HPP

#include <lineairdb/config.h>

#include "index/index_base.h"
#include "index/precision_locking_index/index.hpp"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

class PLIndex final : public IndexBase {
 public:
  PLIndex(Config c, EpochFramework& e) : impl_(c, e) {}

  DataItem* Get(std::string_view key) override { return impl_.Get(key); }

  bool Put(std::string_view key, DataItem&& rhs) override {
    return impl_.Put(key, std::forward<DataItem>(rhs));
  }

  bool Insert(std::string_view key) override { return impl_.Insert(key); }

  bool Delete(std::string_view key) override { return impl_.Delete(key); }

  void ForcePutBlankEntry(std::string_view key) override {
    impl_.ForcePutBlankEntry(key);
  }

  bool EnsureVisibleForSecondaryWrite(std::string_view key) override {
    return impl_.EnsureVisibleForSecondaryWrite(key);
  }

  std::optional<size_t> Scan(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> operation) override {
    return impl_.Scan(begin, end, operation);
  }

  std::optional<size_t> Scan(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem&)> operation) override {
    return impl_.Scan(begin, std::optional<std::string_view>(end), operation);
  }

  std::optional<size_t> ScanReverse(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> operation) override {
    return impl_.ScanReverse(begin, end, operation);
  }

  std::optional<size_t> ScanReverse(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem&)> operation) override {
    return impl_.ScanReverse(begin, std::optional<std::string_view>(end),
                             operation);
  }

  void ForEach(
      std::function<bool(std::string_view, DataItem&)> operation) override {
    impl_.ForEach(operation);
  }

  void WaitForIndexIsLinearizable() override {
    impl_.WaitForIndexIsLinearizable();
  }

 private:
  HashTableWithPrecisionLockingIndex<DataItem> impl_;
};

}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_INDEX_IMPL_PL_INDEX_HPP */
