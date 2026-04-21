#ifndef LINEAIRDB_INDEX_BASE_H
#define LINEAIRDB_INDEX_BASE_H

#include <functional>
#include <optional>
#include <string_view>

#include "types/data_item.hpp"

namespace LineairDB {
namespace Index {

class IndexBase {
 public:
  virtual ~IndexBase() = default;

  // Point operations.
  virtual DataItem* Get(std::string_view key) = 0;
  virtual bool Put(std::string_view key, DataItem&& rhs) = 0;
  virtual bool Insert(std::string_view key) = 0;
  virtual bool Delete(std::string_view key) = 0;

  // Force-insert a blank entry into the point index. PL uses this to seed a
  // value slot that later writes fill in; single-tree backends implement it
  // as an idempotent Insert.
  virtual void ForcePutBlankEntry(std::string_view key) = 0;

  // Make the key visible to future scans even if a prior write left the point
  // index populated but the range index empty (PL's DELETED state).
  // Returns false on phantom anomaly.
  virtual bool EnsureVisibleForSecondaryWrite(std::string_view key) = 0;

  // Range operations. Returning std::nullopt signals a phantom anomaly.
  virtual std::optional<size_t> Scan(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> operation) = 0;
  virtual std::optional<size_t> Scan(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem&)> operation) = 0;
  virtual std::optional<size_t> ScanReverse(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> operation) = 0;
  virtual std::optional<size_t> ScanReverse(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem&)> operation) = 0;

  virtual void ForEach(
      std::function<bool(std::string_view, DataItem&)> operation) = 0;

  virtual void WaitForIndexIsLinearizable() = 0;
};

}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_INDEX_BASE_H */
