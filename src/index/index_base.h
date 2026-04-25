#ifndef LINEAIRDB_INDEX_BASE_H
#define LINEAIRDB_INDEX_BASE_H

#include <cstdint>
#include <functional>
#include <optional>
#include <string_view>
#include <vector>

#include "types/data_item.hpp"

namespace LineairDB {
namespace Index {

class IndexBase;

// Opaque token for deferred phantom detection in tree-structured indexes
// (Masstree). Each entry pairs a backend-private node handle with the
// version observed at scan time; the owning IndexBase re-reads and compares
// at Precommit. PL ignores these entries because it detects phantoms
// synchronously at scan time.
struct NodeVersionEntry {
  IndexBase* owner;
  const void* node_ptr;
  std::uint64_t version;
};

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

  // Range operations. Returning std::nullopt signals a phantom anomaly
  // detected synchronously (PL). Backends that defer phantom checks append
  // per-scan snapshots into `out_versions` for later ValidatePhantoms.
  virtual std::optional<size_t> Scan(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> operation,
      std::vector<NodeVersionEntry>* out_versions = nullptr) = 0;
  virtual std::optional<size_t> Scan(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem&)> operation,
      std::vector<NodeVersionEntry>* out_versions = nullptr) = 0;
  virtual std::optional<size_t> ScanReverse(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> operation,
      std::vector<NodeVersionEntry>* out_versions = nullptr) = 0;
  virtual std::optional<size_t> ScanReverse(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem&)> operation,
      std::vector<NodeVersionEntry>* out_versions = nullptr) = 0;

  virtual void ForEach(
      std::function<bool(std::string_view, DataItem&)> operation) = 0;

  virtual void WaitForIndexIsLinearizable() = 0;

  // Re-check every entry in `entries` whose owner is `this`. Returns true if
  // no phantom has been observed since the scan that produced them. Entries
  // owned by other indexes must be ignored (kept in the combined set so the
  // caller can validate many indexes in one pass).
  virtual bool ValidatePhantoms(
      const std::vector<NodeVersionEntry>& entries) = 0;
};

}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_INDEX_BASE_H */
