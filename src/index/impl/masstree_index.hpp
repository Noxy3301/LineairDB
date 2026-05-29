#ifndef LINEAIRDB_INDEX_IMPL_MASSTREE_INDEX_HPP
#define LINEAIRDB_INDEX_IMPL_MASSTREE_INDEX_HPP

#include <lineairdb/config.h>

#include <cstddef>
#include <cstdint>
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

  DataItem* Get(std::string_view key) override;
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

  bool Purge(std::string_view key,
                               DataItem* expected) override;

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

// ---- Per-transaction epoch pin (helios 2-RPC OCC extension) -------------
// Background: helios's prefetch RPC scans masstree and captures NodeVersion
// entries that the matching commit RPC must re-read. Between those RPCs the
// prefetch handler thread calls MasstreeReleaseThreadEpoch (rcu_stop), which
// removes it from min_active_epoch(); the global ticker then advances
// active_epoch and reclaims leaves retired at >= the prefetch epoch. The
// captured node_ptrs become UAF on commit.
//
// To fix this WITHOUT changing the per-thread RCU model, helios installs a
// per-tx "epoch pin": MasstreeAdvanceEpoch computes
//   active_epoch = min(threadinfo::min_active_epoch(), GetTxPinFloor())
// so any registered pin holds reclamation back the same way a long-lived
// thread enrolment would. The pin is released at commit/abort/connection-
// close/timeout. See `.note/reference/rcu_epoch_background.md`.
//
// Pin lifecycle (Codex Q2 critical):
//   - RegisterTxEpochPinFromCurrentThread MUST be called while the calling
//     thread is still inside its rcu_start/rcu_stop critical section. It
//     reads tls_ti->gc_epoch_ (the value rcu_start STAMPED at entry), not
//     a fresh globalepoch.load(): scans can run for many epochs and a fresh
//     load would publish a later epoch than the leaves were retired at.
//   - The pin epoch is then independent of the thread's gc_epoch_, so it
//     survives MasstreeReleaseThreadEpoch.
//   - ReleaseTxEpochPin must be called at commit/abort/connection-close.
//   - SweepExpiredTxPins is invoked from MasstreeAdvanceEpoch on every tick
//     and erases pins whose TTL has elapsed; the higher-layer TxOccStore
//     observes the absent pin and aborts the tx as expired on commit.
using TxPinKey = std::uint64_t;
// Returns true on success, false if the thread is not currently in a
// masstree RCU critical section (no tls_ti / not enrolled). Caller should
// abort the OCC tx on false.
bool RegisterTxEpochPinFromCurrentThread(TxPinKey key);
void ReleaseTxEpochPin(TxPinKey key);
// True if `key` is currently pinned (Step 5/6 helper: lets the commit RPC
// distinguish "tx was active and survived" from "expired by TTL / proxy
// crash" without racing the map).
bool HasTxPin(TxPinKey key);
// Smallest registered pin epoch (UINT64_MAX when no pins).
std::uint64_t GetTxPinFloor();
// Lease/release a pin for the commit-side validation window. While in_use is
// non-zero the TTL sweep skips the pin, so the commit can safely dereference
// node_ptrs without a sweep race (Codex Step 5/6 P1 fix). Returns false if
// the key is no longer present (caller treats as expired-abort).
bool LeaseTxPinForValidation(TxPinKey key);
void DropTxPinValidationLease(TxPinKey key);
// `now_ns` is the caller's monotonic clock (std::chrono::steady_clock); pins
// with expires_at_ns <= now_ns are removed. Returns the number swept.
std::size_t SweepExpiredTxPins(std::uint64_t now_ns);
// TTL config. Default from HELIOS_PIN_TTL_MS (default 60000). 0 disables TTL.
void SetTxPinTtlMs(std::uint64_t ms);
std::uint64_t GetTxPinTtlMs();

}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_INDEX_IMPL_MASSTREE_INDEX_HPP */
