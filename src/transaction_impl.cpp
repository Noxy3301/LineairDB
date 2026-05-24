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

#include "transaction_impl.h"

#include <lineairdb/transaction.h>

#include <algorithm>
#include <memory>
#include <set>
#include <utility>

#include "concurrency_control/concurrency_control_base.h"
#include "concurrency_control/impl/silo_nwr.hpp"
#include "concurrency_control/impl/two_phase_locking.hpp"
#include "database_impl.h"
#include "types/snapshot.hpp"

namespace LineairDB {

namespace {
thread_local void* current_transaction_context = nullptr;
// Upper 32 bits: thread identity, lower 32 bits: per-thread sequence.
// Wraparound is safe because old transactions have already completed.
thread_local uint64_t tx_context_thread_tag =
    (std::hash<std::thread::id>{}(std::this_thread::get_id()) & 0xFFFFFFFF) << 32;
thread_local uint64_t tx_context_seq = 0;

// True when this transaction wrote a base row inside [begin, end).
bool HasOwnBaseRowWriteInRange(const WriteSetType& write_set,
                               std::string_view table_name,
                               std::string_view begin,
                               const std::optional<std::string_view>& end) {
  for (const auto& snapshot : write_set) {
    if (snapshot.table_name != table_name) continue;
    if (!snapshot.index_name.empty()) continue;

    const std::string_view key(snapshot.key);
    if (key < begin) continue;
    if (end.has_value() && key >= end.value()) continue;
    return true;
  }
  return false;
}
}

void* GetCurrentTransactionContext() { return current_transaction_context; }

Transaction::Impl::Impl(Database::Impl* db_pimpl) noexcept
    : current_status_(TxStatus::Running),
      db_pimpl_(db_pimpl),
      config_ptr_(&db_pimpl_->GetConfig()),
      current_table_(nullptr) {
  current_transaction_context =
      reinterpret_cast<void*>(tx_context_thread_tag | (++tx_context_seq & 0xFFFFFFFF));

  TransactionReferences&& tx = {read_set_, write_set_,
                                db_pimpl_->epoch_framework_, current_status_};

  // WANTFIX for performance
  // Here we allocate one (derived) concurrency control instance per
  // transactions. It may be worse on performance because of heap
  // memory allocation. Need to re-implement with composition or templates.
  switch (config_ptr_->concurrency_control_protocol) {
    case Config::ConcurrencyControl::SiloNWR:
      concurrency_control_ = std::make_unique<ConcurrencyControl::SiloNWR>(
          std::forward<TransactionReferences>(tx));
      break;
    case Config::ConcurrencyControl::Silo:
      concurrency_control_ = std::make_unique<ConcurrencyControl::Silo>(
          std::forward<TransactionReferences>(tx));
      break;
    case Config::ConcurrencyControl::TwoPhaseLocking:
      concurrency_control_ =
          std::make_unique<ConcurrencyControl::TwoPhaseLocking>(
              std::forward<TransactionReferences>(tx));
      break;

    default:
      concurrency_control_ = std::make_unique<ConcurrencyControl::SiloNWR>(
          std::forward<TransactionReferences>(tx));

      break;
  }
}

Transaction::Impl::~Impl() noexcept { current_transaction_context = nullptr; }

void Transaction::Impl::ReconcileOwnInsertWithNodeVersionSet(
    const Index::NodeVersionUpdate& update) {
  if (!update.valid || update.node_ptr == nullptr) return;
  // node_version_set_ is a std::vector and may contain duplicate entries for
  // the same (owner, node_ptr) when a tx scanned the same leaf more than once
  // (overlapping scans, repeated range probes). Silo's reference uses a
  // node-keyed map so each leaf appears once; here we have to advance every
  // matching entry, otherwise a stale entry left at old_version would make
  // commit-time ValidatePhantoms reject our own bump.
  for (auto& entry : node_version_set_) {
    if (entry.owner != update.owner || entry.node_ptr != update.node_ptr)
      continue;
    if (entry.version == update.old_version) {
      // Silo §4.6: own insert advances the node-set entry from v_old to v_new
      // so commit-time ValidatePhantoms does not reject our own bump.
      entry.version = update.new_version;
      continue;
    }
    // Same leaf, but a different version than what we recorded at scan time.
    // A concurrent writer raced between our scan and our insert -> the only
    // sound choice is to abort.
    Abort();
    return;
  }
  // Leaf not in node-set: this scan never observed it, no reconciliation needed.
}

void Transaction::Impl::Reset(Database::Impl* db_pimpl) {
  current_status_ = TxStatus::Running;
  db_pimpl_ = db_pimpl;
  config_ptr_ = &db_pimpl_->GetConfig();
  current_table_ = nullptr;
  // Generate a unique tx_context per transaction so that PrecisionLocking
  // can distinguish successive transactions on the same thread.
  // Upper 32 bits: thread identity, lower 32 bits: per-thread sequence.
  current_transaction_context =
      reinterpret_cast<void*>(tx_context_thread_tag | (++tx_context_seq & 0xFFFFFFFF));
  read_set_.clear();
  write_set_.clear();
  remainingNotNullSkWrites_.clear();
  node_version_set_.clear();

  TransactionReferences new_ref{read_set_, write_set_,
                                db_pimpl_->epoch_framework_, current_status_};
  concurrency_control_->Reset(std::move(new_ref));
}

TxStatus Transaction::Impl::GetCurrentStatus() { return current_status_; }

const std::pair<const std::byte* const, const size_t> Transaction::Impl::Read(
    const std::string_view key) {
  if (IsAborted()) return {nullptr, 0};

  EnsureCurrentTable();

  const auto& table_name = current_table_->GetTableName();

  // Linear search in write_set
  for (auto& snapshot : write_set_) {
    if (snapshot.key == key && snapshot.table_name == table_name &&
        snapshot.index_name.empty()) {
      return std::make_pair(snapshot.data_item_copy.value(),
                            snapshot.data_item_copy.size());
    }
  }

  // Linear search in read_set
  for (auto& snapshot : read_set_) {
    if (snapshot.key == key && snapshot.table_name == table_name &&
        snapshot.index_name.empty()) {
      return std::make_pair(snapshot.data_item_copy.value(),
                            snapshot.data_item_copy.size());
    }
  }

  // Read path: never structurally insert. ForcePutBlankEntry would bump the
  // leaf's vinsert and invalidate any node_version_set_ entry captured by an
  // earlier Scan in this same transaction (or by a concurrent scanner),
  // forcing a spurious phantom abort at commit. Use non-mutating Get; if the
  // key has no slot yet, just report not-found without registering anything.
  auto* index_leaf = current_table_->GetPrimaryIndex().Get(key);
  if (index_leaf == nullptr) {
    return {nullptr, 0};
  }
  Snapshot snapshot = {
      key, nullptr, 0, index_leaf, current_table_->GetTableName(), ""};

  snapshot.data_item_copy = concurrency_control_->Read(key, index_leaf);
  auto& ref = read_set_.emplace_back(std::move(snapshot));
  if (ref.data_item_copy.IsInitialized()) {
    return {ref.data_item_copy.value(), ref.data_item_copy.size()};
  } else {
    return {nullptr, 0};
  }
}

std::vector<std::pair<const std::byte* const, const size_t>>
Transaction::Impl::ReadSecondaryIndex(const std::string_view index_name,
                                      const std::string_view key) {
  if (IsAborted()) return {};
  EnsureCurrentTable();
  Index::SecondaryIndex* index = current_table_->GetSecondaryIndex(index_name);

  if (index == nullptr) {
    Abort();
    return {};
  }

  const auto& table_name = current_table_->GetTableName();

  for (auto& snapshot : write_set_) {
    if (snapshot.key == key && snapshot.table_name == table_name &&
        snapshot.index_name == index_name) {
      std::vector<std::pair<const std::byte* const, const size_t>> result;
      if (!snapshot.data_item_copy.primary_keys().empty()) {
        for (auto& primary_key : snapshot.data_item_copy.primary_keys()) {
          result.emplace_back(
              reinterpret_cast<const std::byte*>(primary_key.data()),
              primary_key.size());
        }
      }
      return result;
    }
  }

  for (auto& snapshot : read_set_) {
    if (snapshot.key == key && snapshot.table_name == table_name &&
        snapshot.index_name == index_name) {
      std::vector<std::pair<const std::byte* const, const size_t>> result;
      if (!snapshot.data_item_copy.primary_keys().empty()) {
        for (auto& primary_key : snapshot.data_item_copy.primary_keys()) {
          result.emplace_back(
              reinterpret_cast<const std::byte*>(primary_key.data()),
              primary_key.size());
        }
      }
      return result;
    }
  }

  // Read path: avoid structural insert — see Read() above.
  DataItem* index_leaf = index->Get(key);
  if (index_leaf == nullptr) {
    return {};
  }
  Snapshot snapshot = {
      key, nullptr, 0, index_leaf, current_table_->GetTableName(), index_name};

  snapshot.data_item_copy = concurrency_control_->Read(key, index_leaf);
  auto& ref = read_set_.emplace_back(std::move(snapshot));
  if (ref.data_item_copy.IsInitialized()) {
    std::vector<std::pair<const std::byte* const, const size_t>> result;
    if (!ref.data_item_copy.primary_keys().empty()) {
      for (auto& primary_key : ref.data_item_copy.primary_keys()) {
        result.emplace_back(
            reinterpret_cast<const std::byte*>(primary_key.data()),
            primary_key.size());
      }
      return result;
    } else {
      return {};
    }
  }
  return {};
}

void Transaction::Impl::Write(const std::string_view key,
                              const std::byte value[], const size_t size) {
  if (IsAborted()) return;

  // TODO: if `size` is larger than Config.internal_buffer_size,
  // then we have to abort this transaction or throw exception
  EnsureCurrentTable();

  const auto& table_name = current_table_->GetTableName();

  bool is_rmf = false;
  for (auto& snapshot : read_set_) {
    if (snapshot.key == key && snapshot.table_name == table_name &&
        snapshot.index_name.empty()) {
      is_rmf = true;
      snapshot.is_read_modify_write = true;
      break;
    }
  }

  for (auto& snapshot : write_set_) {
    if (snapshot.key == key && snapshot.table_name == table_name &&
        snapshot.index_name.empty()) {
      snapshot.data_item_copy.Reset(value, size);
      if (is_rmf) snapshot.is_read_modify_write = true;
      return;
    }
  }

  Index::NodeVersionUpdate own_insert;
  auto* index_leaf =
      current_table_->GetPrimaryIndex().GetOrInsert(key, &own_insert);
  ReconcileOwnInsertWithNodeVersionSet(own_insert);
  if (IsAborted()) return;

  concurrency_control_->Write(key, value, size, index_leaf);
  Snapshot sp(key, value, size, index_leaf, current_table_->GetTableName(), "",
              {});
  sp.pi_ref = &current_table_->GetPrimaryIndex();
  if (is_rmf) sp.is_read_modify_write = true;
  write_set_.emplace_back(std::move(sp));
}

void Transaction::Impl::WriteSecondaryIndex(
    const std::string_view index_name, const std::string_view key,
    const std::byte primary_key_buffer[], const size_t primary_key_size) {
  if (IsAborted()) return;

  EnsureCurrentTable();
  const auto& table_name = current_table_->GetTableName();
  const std::string_view primary_key_view(
      reinterpret_cast<const char*>(primary_key_buffer), primary_key_size);

  // TODO: if `size` is larger than Config.internal_buffer_size,
  // then we have to abort this transaction or throw exception

  Index::SecondaryIndex* index = current_table_->GetSecondaryIndex(index_name);

  // If the index is not registered, abort the transaction
  if (index == nullptr) {
    Abort();
    return;
  }
  const auto index_type = index->GetIndexType();

  // existing key
  // unique constraint check out of the transaction
  Index::NodeVersionUpdate si_own_insert;
  DataItem* index_leaf = index->GetOrInsertForWrite(key, &si_own_insert);
  if (index_leaf == nullptr) {
    Abort();
    return;
  }
  ReconcileOwnInsertWithNodeVersionSet(si_own_insert);
  if (IsAborted()) return;
  if (index_leaf->IsInitialized() && index->IsUnique()) {
    Abort();
    return;
  }

  bool is_rmf = false;
  const DataItem* base_data = nullptr;
  for (auto& snapshot : read_set_) {
    if (snapshot.key == key && snapshot.table_name == table_name &&
        snapshot.index_name == index_name) {
      is_rmf = true;
      base_data = &snapshot.data_item_copy;
      snapshot.is_read_modify_write = true;
      break;
    }
  }

  // unique constraint check in the transaction
  for (auto& snapshot : write_set_) {
    if (snapshot.key != key || snapshot.table_name != table_name ||
        snapshot.index_name != index_name)
      continue;
    if (index->IsUnique()) {
      Abort();
      return;
    }

    snapshot.index_type = index_type;
    snapshot.si_ref = index;
    snapshot.data_item_copy.AddSecondaryIndexValue(primary_key_buffer,
                                                   primary_key_size);
    snapshot.RecordSecondaryIndexDelta(primary_key_view, SecondaryIndexOp::Add);
    if (is_rmf) snapshot.is_read_modify_write = true;
    return;
  }

  if (!is_rmf) {
    Snapshot snapshot = {key,
                         nullptr,
                         0,
                         index_leaf,
                         current_table_->GetTableName(),
                         index_name,
                         0,
                         index_type};
    snapshot.data_item_copy = concurrency_control_->Read(key, index_leaf);
    snapshot.is_read_modify_write = true;

    read_set_.emplace_back(std::move(snapshot));
    base_data = &read_set_.back().data_item_copy;
    is_rmf = true;
  }

  concurrency_control_->Write(key, primary_key_buffer, primary_key_size,
                              index_leaf);
  Snapshot sp(key, nullptr, 0, index_leaf, current_table_->GetTableName(),
              index_name, 0, index_type);
  sp.si_ref = index;

  if (is_rmf) sp.is_read_modify_write = true;
  sp.data_item_copy = *base_data;
  sp.data_item_copy.AddSecondaryIndexValue(primary_key_buffer,
                                           primary_key_size);
  sp.RecordSecondaryIndexDelta(primary_key_view, SecondaryIndexOp::Add);

  write_set_.emplace_back(std::move(sp));
}

void Transaction::Impl::Insert(const std::string_view key,
                               const std::byte value[], const size_t size) {
  if (IsAborted()) return;
  EnsureCurrentTable();

  Index::NodeVersionUpdate own_insert;
  auto inserted = current_table_->GetPrimaryIndex().Insert(key, &own_insert);
  if (!inserted) {
    Abort();
    return;
  }
  ReconcileOwnInsertWithNodeVersionSet(own_insert);
  if (IsAborted()) return;

  // After successful insertion to index, delegate to Write
  Write(key, value, size);
}

void Transaction::Impl::Update(const std::string_view key,
                               const std::byte value[], const size_t size) {
  if (IsAborted()) return;
  EnsureCurrentTable();
  const auto& table_name = current_table_->GetTableName();

  // If the primary-index entry exists in this transaction's write_set_
  // (e.g., Insert() then Update() in the same transaction), Update() should
  // succeed even if the index entry has not been updated yet. Snapshots from
  // WriteSecondaryIndex live in the same write_set_, so filter on the empty
  // index_name to match base-table writes only.
  for (auto& snapshot : write_set_) {
    if (snapshot.key == key && snapshot.table_name == table_name &&
        snapshot.index_name.empty()) {
      // If the key was deleted within this transaction, Update should fail.
      if (!snapshot.data_item_copy.IsInitialized()) {
        Abort();
        return;
      }
      Write(key, value, size);
      return;
    }
  }

  auto* index_leaf = current_table_->GetPrimaryIndex().Get(key);
  if (index_leaf == nullptr || !index_leaf->IsInitialized()) {
    Abort();
    return;
  }

  // After validation, delegate to Write
  Write(key, value, size);
}

void Transaction::Impl::Delete(const std::string_view key) {
  if (IsAborted()) return;
  EnsureCurrentTable();

  // Delete() consists of two deletions: removal from the index (physical)
  //   and initialization of the data item (logical).
  // The reason for this design is that we consider Delete() as
  //   a combination of two writes: a write to the index and a write to the data
  //   item.

  // 1. removal from the index
  bool deleted = current_table_->GetPrimaryIndex().Delete(key);
  if (!deleted) {
    Abort();
    return;
  }
  // 2. initialization of the data item
  this->Update(key, nullptr, 0);
}

const std::optional<size_t> Transaction::Impl::ScanPrimaryIndexWithEarlyStop(
    const std::string_view begin, const std::string_view end,
    std::function<bool(std::string_view, const std::pair<const void*, const size_t>)> operation,
    bool reverse) {
  const auto& table_name = current_table_->GetTableName();
  size_t total_count = 0;

  // Materialize each index entry as a transaction read.
  auto emit_index_row = [&](std::string_view key, DataItem& index_leaf) {
    if (IsAborted()) return true;

    // Reuse read_set_ so repeated reads keep the same value and validation.
    for (auto& snapshot : read_set_) {
      if (snapshot.key != key || snapshot.table_name != table_name || !snapshot.index_name.empty()) {
        continue;
      }
      if (snapshot.data_item_copy.IsInitialized()) {
        std::pair<const void*, const size_t> value_pair = {
          snapshot.data_item_copy.value(), snapshot.data_item_copy.size()};
        total_count++;
        return operation(snapshot.key, value_pair);
      }
      return false;
    }

    // ReadDirect records OCC validation for the row found by the index scan.
    TransactionId scan_tid;
    auto [ptr, sz] = concurrency_control_->ReadDirect(key, &index_leaf, scan_tid);
    if (IsAborted()) return true;

    if (ptr == nullptr || sz == 0) return false;
    total_count++;
    return operation(key, {ptr, sz});
  };

  // Forward and reverse scans share the same row materialization path.
  std::optional<size_t> index_result;
  if (reverse) {
    index_result = current_table_->GetPrimaryIndex().ScanReverse(
        begin, end, emit_index_row, &node_version_set_);
  } else {
    index_result = current_table_->GetPrimaryIndex().Scan(
        begin, end, emit_index_row, &node_version_set_);
  }

  // A missing index result means the scan could not take a safe node snapshot.
  if (!index_result.has_value()) {
    Abort();
    return std::nullopt;
  }
  if (IsAborted()) return std::nullopt;
  return total_count;
}

const std::optional<size_t> Transaction::Impl::Scan(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view,
                       const std::pair<const void*, const size_t>)>
        operation) {
  EnsureCurrentTable();

  // Note: In this Scan implementation, nullptr indicates that the key is
  // deleted or does not exist. SQL NULL values should be handled within the
  // byte array value, not by nullptr.

  const auto& table_name = current_table_->GetTableName();
  // No own writes to merge: scan the index directly and stop on callback.
  if (end.has_value() && !HasOwnBaseRowWriteInRange(write_set_, table_name, begin, end)) {
    return ScanPrimaryIndexWithEarlyStop(begin, end.value(), operation, false);
  }

  // Step 1: Collect keys from index.
  // Keys come out sorted from PrecisionLocking's std::map, so we use a vector
  // instead of std::set to avoid per-key heap allocations.
  std::vector<std::string> index_keys;
  index_keys.reserve(4096);
  auto index_result = current_table_->GetPrimaryIndex().Scan(
      begin, end,
      [&](std::string_view key) {
        index_keys.emplace_back(key);
        return false;  // Continue to collect all keys
      },
      &node_version_set_);

  if (!index_result.has_value()) {
    Abort();
    return std::nullopt;
  }

  // Step 2: Pick out write_set entries inside [begin, end) on the current base table.
  std::vector<std::string> write_set_keys;
  for (const auto& snapshot : write_set_) {
    if (snapshot.table_name != table_name) continue;      // different table
    if (!snapshot.index_name.empty()) continue;           // secondary index entry
    if (snapshot.key < begin) continue;                   // before range
    if (end.has_value() && snapshot.key >= end.value()) continue;  // at/after end
    write_set_keys.emplace_back(snapshot.key);
  }
  std::sort(write_set_keys.begin(), write_set_keys.end());  // std::merge requires sorted inputs

  // Step 3: Merge sorted index_keys and write_set_keys
  std::vector<std::string> all_keys;
  all_keys.reserve(index_keys.size() + write_set_keys.size());
  std::merge(index_keys.begin(), index_keys.end(),
             write_set_keys.begin(), write_set_keys.end(),
             std::back_inserter(all_keys));
  // Deduplicate: index_keys and write_set_keys may overlap (std::set handled this implicitly)
  all_keys.erase(std::unique(all_keys.begin(), all_keys.end()), all_keys.end());

  // Step 4: Process keys in sorted order
  size_t total_count = 0;
  for (const auto& key : all_keys) {
    if (IsAborted()) return std::nullopt;

    // Check write_set first (RYOW: return locally buffered writes)
    bool found_in_write_set = false;
    for (const auto& snapshot : write_set_) {
      if (snapshot.table_name != current_table_->GetTableName()) continue;
      if (!snapshot.index_name.empty()) continue;  // base-table scan only
      if (snapshot.key != key) continue;

      found_in_write_set = true;

      // If the key is deleted within this transaction, skip it
      if (!snapshot.data_item_copy.IsInitialized()) {
        break;
      }

      std::pair<const void*, const size_t> value_pair = {
          snapshot.data_item_copy.value(), snapshot.data_item_copy.size()};
      bool stop_scan = operation(key, value_pair);
      total_count++;
      if (stop_scan) return total_count;

      break;
    }

    if (!found_in_write_set) {
      // Check read_set_ to avoid duplicate validation_set_ registration.
      // Read() already registered this key in validation_set_ via CC::Read();
      // calling ReadDirect again would create a duplicate entry.
      const auto& scan_table = current_table_->GetTableName();
      bool found_in_read_set = false;
      for (auto& snapshot : read_set_) {
        if (snapshot.key != key || snapshot.table_name != scan_table ||
            !snapshot.index_name.empty()) continue;
        found_in_read_set = true;
        if (snapshot.data_item_copy.IsInitialized()) {
          std::pair<const void*, const size_t> value_pair = {
              snapshot.data_item_copy.value(),
              snapshot.data_item_copy.size()};
          bool stop_scan = operation(key, value_pair);
          total_count++;
          if (stop_scan) return total_count;
        }
        break;
      }
      if (found_in_read_set) continue;

      // Not in write_set or read_set: use ReadDirect (zero-copy).
      // This avoids creating a full Snapshot (288B) per scan entry.
      // Validation is tracked via CC's validation_set_ inside ReadDirect.
      // GetOrInsert here usually finds the existing key (the scan that fed
      // us already saw it), so out_update typically stays invalid. We still
      // pass it through so that a concurrent split between scan and this
      // materialization is reconciled with our node-set instead of leaking.
      Index::NodeVersionUpdate scan_own_insert;
      auto* index_leaf =
          current_table_->GetPrimaryIndex().GetOrInsert(key, &scan_own_insert);
      ReconcileOwnInsertWithNodeVersionSet(scan_own_insert);
      if (IsAborted()) return std::nullopt;
      TransactionId scan_tid;
      auto [ptr, sz] = concurrency_control_->ReadDirect(key, index_leaf, scan_tid);
      if (IsAborted()) return std::nullopt;

      if (ptr != nullptr && sz != 0) {
        bool stop_scan = operation(key, {ptr, sz});
        total_count++;
        if (stop_scan) return total_count;
      }
    }
  }

  // TODO: we now only consider the insertion, but we should consider the case
  // for deletions in write_set, when the lineairdb supports delete operation as
  // the public interface of transaction.h.

  return total_count;
};

const std::optional<size_t> Transaction::Impl::ScanReverse(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view,
                       const std::pair<const void*, const size_t>)>
        operation) {
  EnsureCurrentTable();

  // Note: In this Scan implementation, nullptr indicates that the key is
  // deleted or does not exist. SQL NULL values should be handled within the
  // byte array value, not by nullptr.

  const auto& table_name = current_table_->GetTableName();
  // No own writes to merge: scan the index directly and stop on callback.
  if (end.has_value() && !HasOwnBaseRowWriteInRange(write_set_, table_name, begin, end)) {
    return ScanPrimaryIndexWithEarlyStop(begin, end.value(), operation, true);
  }

  // Step 1: Collect keys from index (reverse order from PL's map)
  std::vector<std::string> index_keys;
  index_keys.reserve(4096);
  auto index_result = current_table_->GetPrimaryIndex().ScanReverse(
      begin, end,
      [&](std::string_view key) {
        index_keys.emplace_back(key);
        return false;  // Continue to collect all keys
      },
      &node_version_set_);

  if (!index_result.has_value()) {
    Abort();
    return std::nullopt;
  }

  // Step 2: Pick out write_set entries inside [begin, end) on the current base table.
  std::vector<std::string> write_set_keys;
  for (const auto& snapshot : write_set_) {
    if (snapshot.table_name != table_name) continue;      // different table
    if (!snapshot.index_name.empty()) continue;           // secondary index entry
    if (snapshot.key < begin) continue;                   // before range
    if (end.has_value() && snapshot.key >= end.value()) continue;  // at/after end
    write_set_keys.emplace_back(snapshot.key);
  }
  std::sort(write_set_keys.begin(), write_set_keys.end());  // std::merge requires sorted inputs

  // Step 3: Merge sorted keys (index_keys are in reverse order, so reverse first)
  std::reverse(index_keys.begin(), index_keys.end());
  std::vector<std::string> all_keys;
  all_keys.reserve(index_keys.size() + write_set_keys.size());
  std::merge(index_keys.begin(), index_keys.end(),
             write_set_keys.begin(), write_set_keys.end(),
             std::back_inserter(all_keys));
  // Deduplicate: index_keys and write_set_keys may overlap (std::set handled this implicitly)
  all_keys.erase(std::unique(all_keys.begin(), all_keys.end()), all_keys.end());

  // Step 4: Process keys in reverse order
  size_t total_count = 0;
  for (auto it = all_keys.rbegin(); it != all_keys.rend(); ++it) {
    if (IsAborted()) return std::nullopt;

    const auto& key = *it;
    bool found_in_write_set = false;
    for (const auto& snapshot : write_set_) {
      if (snapshot.table_name != current_table_->GetTableName()) continue;
      if (!snapshot.index_name.empty()) continue;  // base-table scan only
      if (snapshot.key != key) continue;

      found_in_write_set = true;

      if (!snapshot.data_item_copy.IsInitialized()) {
        break;
      }

      std::pair<const void*, const size_t> value_pair = {
          snapshot.data_item_copy.value(), snapshot.data_item_copy.size()};
      bool stop_scan = operation(key, value_pair);
      total_count++;
      if (stop_scan) return total_count;

      break;
    }

    if (!found_in_write_set) {
      // Check read_set_ to avoid duplicate validation_set_ registration.
      const auto& scan_table = current_table_->GetTableName();
      bool found_in_read_set = false;
      for (auto& snapshot : read_set_) {
        if (snapshot.key != key || snapshot.table_name != scan_table ||
            !snapshot.index_name.empty()) continue;
        found_in_read_set = true;
        if (snapshot.data_item_copy.IsInitialized()) {
          std::pair<const void*, const size_t> value_pair = {
              snapshot.data_item_copy.value(),
              snapshot.data_item_copy.size()};
          bool stop_scan = operation(key, value_pair);
          total_count++;
          if (stop_scan) return total_count;
        }
        break;
      }
      if (found_in_read_set) continue;

      auto* index_leaf = current_table_->GetPrimaryIndex().GetOrInsert(key);
      TransactionId scan_tid;
      auto [ptr, sz] = concurrency_control_->ReadDirect(key, index_leaf, scan_tid);
      if (IsAborted()) return std::nullopt;

      if (ptr != nullptr && sz != 0) {
        bool stop_scan = operation(key, {ptr, sz});
        total_count++;
        if (stop_scan) return total_count;
      }
    }
  }

  return total_count;
};

const std::optional<size_t> Transaction::Impl::ScanSecondaryIndex(
    const std::string_view index_name, const std::string_view begin,
    const std::optional<std::string_view> end,
    std::function<bool(std::string_view, const std::vector<std::string>&)>
        operation) {
  EnsureCurrentTable();
  Index::SecondaryIndex* index = current_table_->GetSecondaryIndex(index_name);

  if (index == nullptr) {
    Abort();
    return {};
  }

  const auto& si_table_name = current_table_->GetTableName();

  // Step 1: Collect keys from secondary index.
  // SI Scan returns keys in sorted order, so we can use a vector directly
  // instead of std::set (which allocates per-node).
  std::vector<std::string> index_keys;
  index_keys.reserve(64);
  auto index_result = index->Scan(
      begin, end,
      [&](std::string_view key) {
        index_keys.emplace_back(key);
        return false;  // Continue to collect all keys
      },
      &node_version_set_);

  if (!index_result.has_value()) {
    Abort();
    return std::nullopt;
  }
  std::sort(index_keys.begin(), index_keys.end());

  // Step 2: Pick out write_set entries inside [begin, end) on this secondary index.
  std::vector<std::string> write_set_keys;
  for (const auto& snapshot : write_set_) {
    if (snapshot.table_name != si_table_name) continue;   // different table
    if (snapshot.index_name != index_name) continue;      // different index
    if (snapshot.key < begin) continue;                   // before range
    if (end.has_value() && snapshot.key >= end.value()) continue;  // at/after end
    write_set_keys.emplace_back(snapshot.key);
  }
  std::sort(write_set_keys.begin(), write_set_keys.end());

  // Step 3: Merge sorted index_keys and write_set_keys, deduplicate
  std::vector<std::string> all_keys;
  all_keys.reserve(index_keys.size() + write_set_keys.size());
  std::merge(index_keys.begin(), index_keys.end(),
             write_set_keys.begin(), write_set_keys.end(),
             std::back_inserter(all_keys));
  all_keys.erase(std::unique(all_keys.begin(), all_keys.end()), all_keys.end());

  // Step 4: Process keys in sorted order
  size_t total_count = 0;
  for (const auto& key : all_keys) {
    if (IsAborted()) return std::nullopt;

    // Check if key is in write_set
    bool found_in_write_set = false;
    for (const auto& snapshot : write_set_) {
      if (snapshot.table_name != si_table_name) continue;
      if (snapshot.index_name != index_name) continue;
      if (snapshot.key != key) continue;

      const auto& pks = snapshot.data_item_copy.primary_keys();

      // Skip deleted keys (empty primary_keys means the entry was deleted)
      if (pks.empty()) {
        found_in_write_set = true;
        break;
      }

      total_count++;
      bool stop_scan = operation(key, pks);
      if (stop_scan) return total_count;

      found_in_write_set = true;
      break;
    }

    // If not in write_set, invoke ReadSecondaryIndex
    if (!found_in_write_set) {
      const auto read_result = ReadSecondaryIndex(index_name, key);
      if (IsAborted()) return std::nullopt;

      std::vector<std::string> primary_keys;
      primary_keys.reserve(read_result.size());
      for (const auto& primary_key : read_result) {
        primary_keys.emplace_back(
            reinterpret_cast<const char*>(primary_key.first),
            primary_key.second);
      }

      // Skip deleted keys
      if (primary_keys.empty()) continue;

      total_count++;
      bool stop_scan = operation(key, primary_keys);
      if (stop_scan) return total_count;
    }
  }

  return total_count;
}

const std::optional<size_t> Transaction::Impl::ScanSecondaryIndexReverse(
    const std::string_view index_name, const std::string_view begin,
    const std::optional<std::string_view> end,
    std::function<bool(std::string_view, const std::vector<std::string>&)>
        operation) {
  EnsureCurrentTable();
  Index::SecondaryIndex* index = current_table_->GetSecondaryIndex(index_name);

  if (index == nullptr) {
    Abort();
    return {};
  }

  const auto& si_table_name = current_table_->GetTableName();

  // Step 1: Collect keys from secondary index (sorted order from SI)
  std::vector<std::string> index_keys;
  index_keys.reserve(64);
  auto index_result = index->ScanReverse(
      begin, end,
      [&](std::string_view key) {
        index_keys.emplace_back(key);
        return false;  // Continue to collect all keys
      },
      &node_version_set_);

  if (!index_result.has_value()) {
    Abort();
    return std::nullopt;
  }
  // ScanReverse returns keys in reverse order; re-sort ascending for merge
  std::sort(index_keys.begin(), index_keys.end());

  // Step 2: Pick out write_set entries inside [begin, end) on this secondary index.
  std::vector<std::string> write_set_keys;
  for (const auto& snapshot : write_set_) {
    if (snapshot.table_name != si_table_name) continue;   // different table
    if (snapshot.index_name != index_name) continue;      // different index
    if (snapshot.key < begin) continue;                   // before range
    if (end.has_value() && snapshot.key >= end.value()) continue;  // at/after end
    write_set_keys.emplace_back(snapshot.key);
  }
  std::sort(write_set_keys.begin(), write_set_keys.end());

  // Step 3: Merge sorted keys, deduplicate
  std::vector<std::string> all_keys;
  all_keys.reserve(index_keys.size() + write_set_keys.size());
  std::merge(index_keys.begin(), index_keys.end(),
             write_set_keys.begin(), write_set_keys.end(),
             std::back_inserter(all_keys));
  all_keys.erase(std::unique(all_keys.begin(), all_keys.end()), all_keys.end());

  // Step 4: Process keys in reverse order
  // Forward scan passes primary_keys by const ref directly, but reverse scan
  // needs a reversed copy. Reuse this buffer across iterations to keep capacity.
  std::vector<std::string> reversed_pks;
  size_t total_count = 0;
  for (auto it = all_keys.rbegin(); it != all_keys.rend(); ++it) {
    if (IsAborted()) return std::nullopt;

    const auto& key = *it;
    bool found_in_write_set = false;
    for (const auto& snapshot : write_set_) {
      if (snapshot.table_name != si_table_name) continue;
      if (snapshot.index_name != index_name) continue;
      if (snapshot.key != key) continue;

      const auto& pks = snapshot.data_item_copy.primary_keys();

      // Skip deleted keys (empty primary_keys means the entry was deleted)
      if (pks.empty()) {
        found_in_write_set = true;
        break;
      }

      reversed_pks.assign(pks.rbegin(), pks.rend());
      total_count++;
      bool stop_scan = operation(key, reversed_pks);
      if (stop_scan) return total_count;

      found_in_write_set = true;
      break;
    }

    if (!found_in_write_set) {
      const auto read_result = ReadSecondaryIndex(index_name, key);
      if (IsAborted()) return std::nullopt;

      reversed_pks.clear();
      reversed_pks.reserve(read_result.size());
      for (const auto& primary_key : read_result) {
        reversed_pks.emplace_back(
            reinterpret_cast<const char*>(primary_key.first),
            primary_key.second);
      }
      std::reverse(reversed_pks.begin(), reversed_pks.end());

      // Skip deleted keys
      if (reversed_pks.empty()) continue;

      total_count++;
      bool stop_scan = operation(key, reversed_pks);
      if (stop_scan) return total_count;
    }
  }

  return total_count;
}

void Transaction::Impl::DeleteSecondaryIndex(
    const std::string_view index_name, const std::string_view secondary_key,
    const std::byte primary_key_buffer[], const size_t primary_key_size) {
  if (IsAborted()) return;

  EnsureCurrentTable();
  const std::string_view primary_key_view(
      reinterpret_cast<const char*>(primary_key_buffer), primary_key_size);

  Index::SecondaryIndex* index = current_table_->GetSecondaryIndex(index_name);
  if (index == nullptr) {
    Abort();
    return;
  }
  const auto index_type = index->GetIndexType();

  // delete the primary key from the data item associated
  // with the old secondary key ==========
  Index::NodeVersionUpdate si_own_insert_del;
  auto index_leaf = index->GetOrInsert(secondary_key, &si_own_insert_del);
  ReconcileOwnInsertWithNodeVersionSet(si_own_insert_del);
  if (IsAborted()) return;
  bool found_in_write_set = false;

  bool is_rmf = false;
  const DataItem* base_data = nullptr;
  for (auto& snapshot : read_set_) {
    if (snapshot.key == secondary_key &&
        snapshot.table_name == current_table_->GetTableName() &&
        snapshot.index_name == index_name) {
      is_rmf = true;
      base_data = &snapshot.data_item_copy;
      snapshot.is_read_modify_write = true;
      break;
    }
  }

  // case A: old_key is in write_set
  for (auto& snapshot : write_set_) {
    if (snapshot.key != secondary_key ||
        snapshot.table_name != current_table_->GetTableName() ||
        snapshot.index_name != index_name)
      continue;

    found_in_write_set = true;
    snapshot.index_type = index_type;
    snapshot.si_ref = index;
    snapshot.data_item_copy.RemoveSecondaryIndexValue(primary_key_buffer,
                                                      primary_key_size);
    snapshot.RecordSecondaryIndexDelta(primary_key_view,
                                       SecondaryIndexOp::Remove);
    if (snapshot.data_item_copy.primary_keys().empty()) {
      if (!index->Delete(secondary_key)) {
        Abort();
        return;
      }
    }
    if (is_rmf) snapshot.is_read_modify_write = true;
    break;
  }

  // case B: old_key is not in write_set
  if (!found_in_write_set) {
    if (!is_rmf) {
      Snapshot snapshot = {secondary_key,
                           nullptr,
                           0,
                           index_leaf,
                           current_table_->GetTableName(),
                           index_name,
                           0,
                           index_type};

      snapshot.data_item_copy =
          concurrency_control_->Read(secondary_key, index_leaf);
      read_set_.emplace_back(std::move(snapshot));
      base_data = &read_set_.back().data_item_copy;
    }
    Snapshot sp(secondary_key, nullptr, 0, index_leaf,
                current_table_->GetTableName(), index_name, 0, index_type);
    sp.si_ref = index;
    sp.data_item_copy = *base_data;
    sp.data_item_copy.RemoveSecondaryIndexValue(primary_key_buffer,
                                                primary_key_size);

    if (sp.data_item_copy.primary_keys().empty()) {
      if (!index->Delete(secondary_key)) {
        Abort();
        return;
      }
    }

    sp.RecordSecondaryIndexDelta(primary_key_view, SecondaryIndexOp::Remove);
    if (is_rmf) sp.is_read_modify_write = true;
    write_set_.emplace_back(std::move(sp));
  }
}

void Transaction::Impl::UpdateSecondaryIndex(
    const std::string_view index_name, const std::string_view old_secondary_key,
    const std::string_view new_secondary_key,
    const std::byte primary_key_buffer[], const size_t primary_key_size) {
  if (IsAborted()) return;

  EnsureCurrentTable();
  const std::string_view primary_key_view(
      reinterpret_cast<const char*>(primary_key_buffer), primary_key_size);

  Index::SecondaryIndex* index = current_table_->GetSecondaryIndex(index_name);
  if (index == nullptr) {
    Abort();
    return;
  }
  const auto index_type = index->GetIndexType();

  // ========== Phase 1: delete the primary key from the data item
  Index::NodeVersionUpdate si_own_insert_old;
  auto old_leaf = index->GetOrInsert(old_secondary_key, &si_own_insert_old);
  ReconcileOwnInsertWithNodeVersionSet(si_own_insert_old);
  if (IsAborted()) return;
  bool old_found_in_write_set = false;

  bool is_rmf_old_key = false;
  const DataItem* base_data_old_key = nullptr;
  for (auto& snapshot : read_set_) {
    if (snapshot.key == old_secondary_key &&
        snapshot.table_name == current_table_->GetTableName() &&
        snapshot.index_name == index_name) {
      is_rmf_old_key = true;
      base_data_old_key = &snapshot.data_item_copy;
      snapshot.is_read_modify_write = true;
      break;
    }
  }

  // case A: old_key is in write_set
  for (auto& snapshot : write_set_) {
    if (snapshot.key != old_secondary_key ||
        snapshot.table_name != current_table_->GetTableName() ||
        snapshot.index_name != index_name)
      continue;

    old_found_in_write_set = true;
    snapshot.index_type = index_type;
    snapshot.si_ref = index;
    snapshot.data_item_copy.RemoveSecondaryIndexValue(primary_key_buffer,
                                                      primary_key_size);
    snapshot.RecordSecondaryIndexDelta(primary_key_view,
                                       SecondaryIndexOp::Remove);
    if (snapshot.data_item_copy.primary_keys().empty()) {
      if (!index->Delete(old_secondary_key)) {
        Abort();
        return;
      }
    }
    if (is_rmf_old_key) snapshot.is_read_modify_write = true;
    break;
  }

  // case B: old_key is not in write_set
  if (!old_found_in_write_set) {
    if (!is_rmf_old_key) {
      Snapshot snapshot = {old_secondary_key,
                           nullptr,
                           0,
                           old_leaf,
                           current_table_->GetTableName(),
                           index_name,
                           0,
                           index_type};

      snapshot.data_item_copy =
          concurrency_control_->Read(old_secondary_key, old_leaf);
      read_set_.emplace_back(std::move(snapshot));
      base_data_old_key = &read_set_.back().data_item_copy;
    }
    Snapshot sp(old_secondary_key, nullptr, 0, old_leaf,
                current_table_->GetTableName(), index_name, 0, index_type);
    sp.si_ref = index;
    sp.data_item_copy = *base_data_old_key;
    sp.data_item_copy.RemoveSecondaryIndexValue(primary_key_buffer,
                                                primary_key_size);

    if (sp.data_item_copy.primary_keys().empty()) {
      if (!index->Delete(old_secondary_key)) {
        Abort();
        return;
      }
    }

    concurrency_control_->Write(old_secondary_key, primary_key_buffer,
                                primary_key_size, old_leaf);
    sp.RecordSecondaryIndexDelta(primary_key_view, SecondaryIndexOp::Remove);
    if (is_rmf_old_key) sp.is_read_modify_write = true;
    write_set_.emplace_back(std::move(sp));
  }

  // ========== Phase 2: add the primary key to the data item associated with
  // the new secondary key ==========
  Index::NodeVersionUpdate si_own_insert_new;
  auto new_leaf =
      index->GetOrInsertForWrite(new_secondary_key, &si_own_insert_new);
  if (new_leaf == nullptr) {
    Abort();
    return;
  }
  ReconcileOwnInsertWithNodeVersionSet(si_own_insert_new);
  if (IsAborted()) return;
  // unique constraint check out of the transaction
  if (new_leaf->IsInitialized() && index->IsUnique()) {
    Abort();
    return;
  }
  bool new_found_in_write_set = false;

  bool is_rmf_new_key = false;
  const DataItem* base_data_new_key = nullptr;
  for (auto& snapshot : read_set_) {
    if (snapshot.key == new_secondary_key &&
        snapshot.table_name == current_table_->GetTableName() &&
        snapshot.index_name == index_name) {
      is_rmf_new_key = true;
      snapshot.is_read_modify_write = true;
      base_data_new_key = &snapshot.data_item_copy;
      break;
    }
  }

  // case: new_key is in write_set
  for (auto& snapshot : write_set_) {
    if (snapshot.key != new_secondary_key ||
        snapshot.table_name != current_table_->GetTableName() ||
        snapshot.index_name != index_name)
      continue;

    // unique constraint check in the transaction
    if (index->IsUnique()) {
      Abort();
      return;
    }

    new_found_in_write_set = true;
    snapshot.index_type = index_type;
    snapshot.si_ref = index;
    snapshot.data_item_copy.AddSecondaryIndexValue(primary_key_buffer,
                                                   primary_key_size);
    snapshot.RecordSecondaryIndexDelta(primary_key_view, SecondaryIndexOp::Add);
    if (is_rmf_new_key) snapshot.is_read_modify_write = true;
    break;
  }

  // case: new_key is not in write_set
  if (!new_found_in_write_set) {
    if (!is_rmf_new_key) {
      Snapshot snapshot = {new_secondary_key,
                           nullptr,
                           0,
                           new_leaf,
                           current_table_->GetTableName(),
                           index_name,
                           0,
                           index_type};

      snapshot.data_item_copy =
          concurrency_control_->Read(new_secondary_key, new_leaf);
      read_set_.emplace_back(std::move(snapshot));
      base_data_new_key = &read_set_.back().data_item_copy;
    }
    Snapshot sp(new_secondary_key, nullptr, 0, new_leaf,
                current_table_->GetTableName(), index_name, 0, index_type);
    sp.si_ref = index;
    sp.data_item_copy = *base_data_new_key;
    sp.data_item_copy.AddSecondaryIndexValue(primary_key_buffer,
                                             primary_key_size);
    concurrency_control_->Write(new_secondary_key, primary_key_buffer,
                                primary_key_size, new_leaf);

    sp.RecordSecondaryIndexDelta(primary_key_view, SecondaryIndexOp::Add);
    if (is_rmf_new_key) sp.is_read_modify_write = true;

    write_set_.emplace_back(std::move(sp));
  }
}

void Transaction::Impl::Abort() {
  if (!IsAborted()) {
    current_status_ = TxStatus::Aborted;
    concurrency_control_->Abort();
    concurrency_control_->PostProcessing(TxStatus::Aborted);
  }
}
bool Transaction::Impl::Precommit() {
  if (IsAborted()) return false;

  // Install deferred phantom validator. Silo runs this at the serial
  // point (under write locks, after AntiDepValidation) so concurrent
  // masstree structural changes happening-before commit are observed.
  // Running the check earlier admits a race window between validation
  // and lock acquisition; running it here keeps the protocol strict-
  // serializable. PL's ValidatePhantoms is a no-op.
  concurrency_control_->SetPreCommitValidator([this]() {
    if (node_version_set_.empty()) return true;
    std::unordered_set<Index::IndexBase*> owners;
    for (const auto& e : node_version_set_) owners.insert(e.owner);
    for (auto* owner : owners) {
      if (!owner->ValidatePhantoms(node_version_set_)) return false;
    }
    return true;
  });

  const bool need_to_checkpoint =
      (db_pimpl_->GetConfig().enable_checkpointing &&
       db_pimpl_->IsNeedToCheckpointing(
           db_pimpl_->epoch_framework_.GetMyThreadLocalEpoch()));
  bool committed = concurrency_control_->Precommit(need_to_checkpoint);
  return committed;
}

void Transaction::Impl::PostProcessing(TxStatus status) {
  if (status == TxStatus::Aborted) current_status_ = TxStatus::Aborted;
  concurrency_control_->PostProcessing(status);
}

void Transaction::Impl::EnsureCurrentTable() {
  if (current_table_ == nullptr) {
    current_table_ =
        db_pimpl_->GetTable(config_ptr_->anonymous_table_name).value();
  }
}

bool Transaction::Impl::SetTable(const std::string_view table_name) {
  auto table = db_pimpl_->GetTable(table_name);
  if (!table.has_value()) {
    return false;  // Table not found
  }
  current_table_ = table.value();
  return true;
}

TxStatus Transaction::GetCurrentStatus() {
  return tx_pimpl_->GetCurrentStatus();
}

const std::pair<const std::byte* const, const size_t> Transaction::Read(
    const std::string_view key) {
  return tx_pimpl_->Read(key);
}

std::vector<std::pair<const std::byte* const, const size_t>>
Transaction::ReadSecondaryIndex(const std::string_view index_name,
                                const std::string_view key) {
  return tx_pimpl_->ReadSecondaryIndex(index_name, key);
}

void Transaction::Write(const std::string_view key, const std::byte value[],
                        const size_t size) {
  tx_pimpl_->Write(key, value, size);
}
void Transaction::WriteSecondaryIndex(const std::string_view index_name,
                                      const std::string_view key,
                                      const std::byte primary_key_buffer[],
                                      const size_t primary_key_size) {
  tx_pimpl_->WriteSecondaryIndex(index_name, key, primary_key_buffer,
                                 primary_key_size);
}

void Transaction::DeleteSecondaryIndex(const std::string_view index_name,
                                       const std::string_view secondary_key,
                                       const std::byte primary_key_buffer[],
                                       const size_t primary_key_size) {
  tx_pimpl_->DeleteSecondaryIndex(index_name, secondary_key, primary_key_buffer,
                                  primary_key_size);
}

void Transaction::UpdateSecondaryIndex(const std::string_view index_name,
                                       const std::string_view old_secondary_key,
                                       const std::string_view new_secondary_key,
                                       const std::byte primary_key_buffer[],
                                       const size_t primary_key_size) {
  tx_pimpl_->UpdateSecondaryIndex(index_name, old_secondary_key,
                                  new_secondary_key, primary_key_buffer,
                                  primary_key_size);
}

void Transaction::Insert(const std::string_view key, const std::byte value[],
                         const size_t size) {
  tx_pimpl_->Insert(key, value, size);
}
void Transaction::Update(const std::string_view key, const std::byte value[],
                         const size_t size) {
  tx_pimpl_->Update(key, value, size);
}
void Transaction::Delete(const std::string_view key) { tx_pimpl_->Delete(key); }
const std::optional<size_t> Transaction::Scan(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view,
                       const std::pair<const void*, const size_t>)>
        operation) {
  return tx_pimpl_->Scan(begin, end, operation);
};

const std::optional<size_t> Transaction::ScanReverse(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view,
                       const std::pair<const void*, const size_t>)>
        operation) {
  return tx_pimpl_->ScanReverse(begin, end, operation);
};

const std::optional<size_t> Transaction::ScanSecondaryIndex(
    const std::string_view index_name, const std::string_view begin,
    const std::optional<std::string_view> end,
    std::function<bool(std::string_view, const std::vector<std::string>&)>
        operation) {
  return tx_pimpl_->ScanSecondaryIndex(index_name, begin, end, operation);
}

const std::optional<size_t> Transaction::ScanSecondaryIndexReverse(
    const std::string_view index_name, const std::string_view begin,
    const std::optional<std::string_view> end,
    std::function<bool(std::string_view, const std::vector<std::string>&)>
        operation) {
  return tx_pimpl_->ScanSecondaryIndexReverse(index_name, begin, end,
                                              operation);
}

void Transaction::Abort() { tx_pimpl_->Abort(); }
bool Transaction::SetTable(const std::string_view table_name) {
  return tx_pimpl_->SetTable(table_name);
}
bool Transaction::Precommit() { return tx_pimpl_->Precommit(); }

Transaction::Transaction(void* db_pimpl) noexcept
    : tx_pimpl_(
          std::make_unique<Impl>(reinterpret_cast<Database::Impl*>(db_pimpl))) {
}
Transaction::~Transaction() noexcept = default;

}  // namespace LineairDB
