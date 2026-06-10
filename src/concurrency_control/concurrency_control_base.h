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

#ifndef LINEAIRDB_CONCURRENCY_CONTROL_BASE_H
#define LINEAIRDB_CONCURRENCY_CONTROL_BASE_H

#include <lineairdb/tx_status.h>

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>

#include "index/concurrent_table.h"
#include "types/data_item.hpp"
#include "util/epoch_framework.hpp"

namespace LineairDB {
struct TransactionReferences {
  ReadSetType& read_set_ref_;
  WriteSetType& write_set_ref_;
  EpochFramework& epoch_framework_ref_;
  TxStatus& current_status_ref_;
  std::function<void(const Snapshot&, TransactionId)> register_deferred_purge_;
};
class ConcurrencyControlBase {
 public:
  ConcurrencyControlBase(TransactionReferences&& tx) : tx_ref_(tx) {}
  virtual ~ConcurrencyControlBase(){};
  virtual const DataItem Read(std::string_view, DataItem*) = 0;
  // Zero-copy read for Scan. Returns pointer + size, no DataItem copy.
  // Scan only forwards raw bytes and never inspects DataItem internals
  // (e.g. primary_keys), so the pointer is sufficient.
  // Use Read() when the full DataItem structure is needed.
  virtual std::pair<const std::byte*, size_t> ReadDirect(
      std::string_view key, DataItem* index_leaf,
      TransactionId& out_tid) = 0;
  virtual void Write(const std::string_view key, const std::byte* const value,
                     const size_t size, DataItem*) = 0;
  virtual void Abort() = 0;
  virtual bool Precommit(bool) = 0;
  virtual void PostProcessing(TxStatus) = 0;
  virtual void Reset(TransactionReferences&& new_ref) = 0;

  bool IsReadOnly() { return (0 == tx_ref_.write_set_ref_.size()); }
  bool IsWriteOnly() { return (0 == tx_ref_.read_set_ref_.size()); }

  // Hook for index-layer checks that must run at the CC's serial point
  // (after read-set validation and under write locks, before buffer
  // update). Used by Masstree's deferred phantom validation. Returning
  // false aborts the tx. PL-backed txs leave this unset.
  using PreCommitValidator = std::function<bool()>;
  void SetPreCommitValidator(PreCommitValidator v) {
    pre_commit_validator_ = std::move(v);
  }

 protected:
  TransactionReferences tx_ref_;
  PreCommitValidator pre_commit_validator_;
};
}  // namespace LineairDB

#endif /* LINEAIRDB_CONCURRENCY_CONTROL_BASE_H */
