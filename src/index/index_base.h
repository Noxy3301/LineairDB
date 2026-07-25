#ifndef LINEAIRDB_INDEX_BASE_H
#define LINEAIRDB_INDEX_BASE_H

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <lineairdb/pax_store.h>

#include "types/data_item.hpp"

namespace LineairDB {
namespace Index {

class IndexBase;

// A stable observation of one tree node resolved from a by-value locator.
// Incarnation distinguishes different allocations that occupied the same
// address, while version detects changes within one allocation's lifetime.
struct NodeVersionObservation {
  std::uint64_t incarnation;
  std::uint64_t version;
};

// Opaque token for deferred phantom detection in tree-structured indexes
// (Masstree). Each entry pairs a backend-private node handle with the
// version observed at scan time; the owning IndexBase re-reads and compares
// at Precommit. The locator fields are a by-value route to the same leaf for
// stateless validation; native validation continues to use node_ptr while its
// RCU epoch remains active. PL ignores these entries because it detects
// phantoms synchronously at scan time.
struct NodeVersionEntry {
  IndexBase* owner;
  const void* node_ptr;
  std::uint64_t version;
  std::uint64_t incarnation;
  std::string anchor_key;
  std::uint32_t layer_prefix_length;
};

// Reports what an Insert/Put/ForcePutBlankEntry actually did at the leaf
// version level. Used by OCC backends to apply the Silo §4.6 self-bump rule:
// if the affected leaf is already in the transaction's node-set with
// `old_version`, advance that entry to `new_version` (and only abort the tx
// if some other concurrent writer raced between the scan and our insert).
// `valid=false` means the call did not bump any leaf version (e.g. an
// in-place overwrite, or the call was a no-op).
struct NodeVersionUpdate {
  IndexBase* owner = nullptr;
  const void* node_ptr = nullptr;
  std::uint64_t old_version = 0;
  std::uint64_t new_version = 0;
  std::uint64_t incarnation = 0;
  std::string anchor_key;
  std::uint32_t layer_prefix_length = 0;
  bool valid = false;
};

class IndexBase {
 public:
  virtual ~IndexBase() = default;

  /**
   * @brief Routes future blank primary rows through a table PAX store.
   *
   * @details Backends without PAX support ignore this hook, so their rows keep
   * the ordinary heap-backed DataBuffer layout. Secondary indexes never set a
   * PaxStore because they store index metadata rather than table row payloads.
   */
  virtual void SetPaxStore(Pax::PaxStore* /*store*/) {}

  // Point operations. The optional `out_update` lets the OCC layer learn
  // whether the call structurally bumped a leaf version, so it can apply
  // the Silo §4.6 own-write node-set rule (advance any matching node-set
  // entry from old_version to new_version, abort only on a real race).
  // Backends that do not track leaf versions (PL) leave `out_update->valid`
  // false.

  // When the key has no slot, backends that track leaf versions append the
  // leaf that would hold it, with that leaf's version, to `out_versions`.
  // A caller that passes its node set therefore puts a failed lookup under
  // the same commit-time check as a scan: an insert into that leaf moves its
  // version, and ValidatePhantoms rejects the entry. A lookup that finds a
  // slot appends nothing, and so do backends without leaf versions (PL).
  virtual DataItem* Get(
      std::string_view key,
      std::vector<NodeVersionEntry>* out_versions = nullptr) = 0;
  virtual bool Put(std::string_view key, DataItem&& rhs,
                   NodeVersionUpdate* out_update = nullptr) = 0;
  virtual bool Insert(std::string_view key,
                      NodeVersionUpdate* out_update = nullptr) = 0;
  virtual bool Delete(std::string_view key) = 0;

  // Force-insert a blank entry into the point index. PL uses this to seed a
  // value slot that later writes fill in; single-tree backends implement it
  // as an idempotent Insert.
  virtual void ForcePutBlankEntry(std::string_view key,
                                   NodeVersionUpdate* out_update = nullptr) = 0;

  // Make the key visible to future scans even if a prior write left the point
  // index populated but the range index empty (PL's DELETED state).
  // Returns false on phantom anomaly.
  virtual bool EnsureVisibleForSecondaryWrite(
      std::string_view key, NodeVersionUpdate* out_update = nullptr) = 0;

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

  // Resolve a by-value node locator without dereferencing a read-time pointer.
  // Backends without durable node identities return std::nullopt so callers
  // cannot mistake unsupported validation for an unchanged node.
  virtual std::optional<NodeVersionObservation> ReadNodeVersion(
      std::string_view /*anchor_key*/,
      std::uint32_t /*layer_prefix_length*/) {
    return std::nullopt;
  }

  // Structurally remove a committed tombstone from the index. Called by the
  // deferred purge reaper only, after it has locked `expected`, verified the
  // delete TID, and confirmed the key still resolves to the same DataItem.
  // `retired_tid` is published on the removed item before it is RCU-retired.
  // Default no-op for backends without physical reclamation (PL).
  virtual bool Purge(std::string_view /*key*/, DataItem* /*expected*/,
                     TransactionId /*retired_tid*/ = {}) {
    return false;
  }
};

}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_INDEX_BASE_H */
