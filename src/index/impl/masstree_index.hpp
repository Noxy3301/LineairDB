#ifndef LINEAIRDB_INDEX_IMPL_MASSTREE_INDEX_HPP
#define LINEAIRDB_INDEX_IMPL_MASSTREE_INDEX_HPP

#include <lineairdb/config.h>

#include <memory>

#include "index/index_base.h"
#include "util/epoch_framework.hpp"

namespace LineairDB {
namespace Index {

// PImpl wrapper around masstree-beta. Masstree headers are confined to
// masstree_index.cpp; this header stays free of masstree to avoid leaking
// its templates / macros through index_factory.hpp -> secondary_index.h
// into the rest of LDB (and through there, into tests that do not have
// masstree on their include path).
class MasstreeIndex final : public IndexBase {
 public:
  MasstreeIndex(Config c, EpochFramework& e);
  ~MasstreeIndex() override;

  void SetPaxStore(Pax::PaxStore* store) override;

  DataItem* Get(std::string_view key,
                std::vector<NodeVersionEntry>* out_versions = nullptr) override;
  bool Put(std::string_view key, DataItem&& rhs,
           NodeVersionUpdate* out_update = nullptr) override;
  bool Insert(std::string_view key,
              NodeVersionUpdate* out_update = nullptr) override;
  bool Delete(std::string_view key) override;

  void ForcePutBlankEntry(std::string_view key,
                          NodeVersionUpdate* out_update = nullptr) override;
  bool EnsureVisibleForSecondaryWrite(
      std::string_view key, NodeVersionUpdate* out_update = nullptr) override;

  std::optional<size_t> Scan(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> operation,
      std::vector<NodeVersionEntry>* out_versions = nullptr) override;
  std::optional<size_t> Scan(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem&)> operation,
      std::vector<NodeVersionEntry>* out_versions = nullptr) override;
  std::optional<size_t> ScanReverse(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> operation,
      std::vector<NodeVersionEntry>* out_versions = nullptr) override;
  std::optional<size_t> ScanReverse(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem&)> operation,
      std::vector<NodeVersionEntry>* out_versions = nullptr) override;

  void ForEach(
      std::function<bool(std::string_view, DataItem&)> operation) override;

  void WaitForIndexIsLinearizable() override;

  bool ValidatePhantoms(
      const std::vector<NodeVersionEntry>& entries) override;

  std::optional<NodeVersionObservation> ReadNodeVersion(
      std::string_view anchor_key,
      std::uint32_t layer_prefix_length) override;

  bool Purge(std::string_view key, DataItem* expected,
             TransactionId retired_tid = {}) override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// Hooks into masstree-beta's RCU machinery. masstree's globalepoch needs
// to be driven by the host's epoch ticker (MasstreeAdvanceEpoch). Threads
// that touch the tree enrol implicitly via masstree ops; they close their
// critical section by calling MasstreeReleaseThreadEpoch at a safe
// boundary (no raw DataItem* / leaf pointer from this section can be used
// past the release). There is intentionally no "advance without release"
// API — re-stamping gc_epoch_ mid-section would let RCU reclaim pointers
// the caller is still using.
// Both per-thread functions are no-ops when no masstree threadinfo has
// been initialised on the current thread.
void MasstreeAdvanceEpoch();
void MasstreeReleaseThreadEpoch();
// Like MasstreeReleaseThreadEpoch but pessimistically advances the global
// epoch in a loop so the calling thread's limbo gets fully drained before
// it exits. Heavier than a regular release; intended for connection-close
// paths only.
void MasstreeFullyDrainThread();

// Replaces the process-wide allocator state and returns its prior value.
// This hook is confined to internal tests that exercise counter exhaustion.
std::uint64_t MasstreeSetNextLeafIncarnationForTesting(
    std::uint64_t next_incarnation);

}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_INDEX_IMPL_MASSTREE_INDEX_HPP */
