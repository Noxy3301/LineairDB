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

  // PL detects phantoms synchronously during a scan and keeps no node set,
  // so it reports no leaf versions.
  DataItem* Get(
      std::string_view key,
      std::vector<NodeVersionEntry>* /*out_versions*/ = nullptr) override {
    return impl_.Get(key);
  }

  bool Put(std::string_view key, DataItem&& rhs,
           NodeVersionUpdate* /*out_update*/ = nullptr) override {
    return impl_.Put(key, std::forward<DataItem>(rhs));
  }

  bool Insert(std::string_view key,
              NodeVersionUpdate* /*out_update*/ = nullptr) override {
    return impl_.Insert(key);
  }

  bool Delete(std::string_view key) override { return impl_.Delete(key); }

  void ForcePutBlankEntry(std::string_view key,
                          NodeVersionUpdate* /*out_update*/ = nullptr) override {
    impl_.ForcePutBlankEntry(key);
  }

  bool EnsureVisibleForSecondaryWrite(
      std::string_view key,
      NodeVersionUpdate* /*out_update*/ = nullptr) override {
    return impl_.EnsureVisibleForSecondaryWrite(key);
  }

  std::optional<size_t> Scan(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> operation,
      std::vector<NodeVersionEntry>* /*out_versions*/ = nullptr) override {
    return impl_.Scan(begin, end, operation);
  }

  std::optional<size_t> Scan(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem&)> operation,
      std::vector<NodeVersionEntry>* /*out_versions*/ = nullptr) override {
    return impl_.Scan(begin, std::optional<std::string_view>(end), operation);
  }

  std::optional<size_t> ScanReverse(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> operation,
      std::vector<NodeVersionEntry>* /*out_versions*/ = nullptr) override {
    return impl_.ScanReverse(begin, end, operation);
  }

  std::optional<size_t> ScanReverse(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem&)> operation,
      std::vector<NodeVersionEntry>* /*out_versions*/ = nullptr) override {
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

  // PL already rejects phantoms at Scan time (nullopt return), so deferred
  // validation is a no-op.
  bool ValidatePhantoms(
      const std::vector<NodeVersionEntry>& /*entries*/) override {
    return true;
  }

 private:
  HashTableWithPrecisionLockingIndex<DataItem> impl_;
};

}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_INDEX_IMPL_PL_INDEX_HPP */
