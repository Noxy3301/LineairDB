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

#ifndef LINEAIRDB_SILO_NWR_H
#define LINEAIRDB_SILO_NWR_H

#include <lineairdb/tx_status.h>

#include <atomic>
#include <cstddef>
#include <cstring>
#include <xmmintrin.h>
#include <vector>

#include "concurrency_control/concurrency_control_base.h"
#include "concurrency_control/pivot_object.hpp"
#include "pax/version_store.hpp"
#include "types/data_item.hpp"
#include "types/definitions.h"
#include "util/debug_sync.hpp"

namespace LineairDB {

namespace ConcurrencyControl {

template <bool EnableNWR = true>
class SiloNWRTyped final : public ConcurrencyControlBase {
  using NWRObjectType = std::atomic<NWRPivotObject>;

 private:
  struct ValidationItem {
    const DataItem* item_p_cache;
    TransactionId transaction_id;
  };
  struct PivotObjectSnapshot {
    DataItem* item_p_cache;
    NWRPivotObject pv_snapshot;
    enum SnapshotFrom { READSET, WRITESET };
    SnapshotFrom set_type;
  };

  std::vector<ValidationItem> validation_set_;
  NWRValidationResult nwr_validation_result_;
  NWRPivotObject my_pivot_object_;
  std::vector<PivotObjectSnapshot> pivot_object_snapshots_;

 public:
  SiloNWRTyped(TransactionReferences&& tx)
      : ConcurrencyControlBase(std::forward<TransactionReferences&&>(tx)),
        nwr_validation_result_(NWRValidationResult::NOT_YET_VALIDATED){};
  ~SiloNWRTyped() final override{};

  // TransactionReferences has reference members, so normal assignment (=) is
  // not possible. Destroy-then-placement-new rebinds the references in place.
  void Reset(TransactionReferences&& new_ref) override {
    tx_ref_.~TransactionReferences();
    new (&tx_ref_) TransactionReferences(std::move(new_ref));
    validation_set_.clear();
    aborted_by_duplicate_key_ = false;
    nwr_validation_result_ = NWRValidationResult::NOT_YET_VALIDATED;
    my_pivot_object_ = NWRPivotObject();
    pivot_object_snapshots_.clear();
    pre_commit_validator_ = {};
  }

  const DataItem Read(const std::string_view,
                      DataItem* index_leaf) final override {
    assert(index_leaf != nullptr);

    DataItem snapshot;
    for (;;) {
      auto tx_id = index_leaf->transaction_id.load();

      if (tx_id.tid & 1u) {  // locked
        _mm_pause();
        continue;
      }

      snapshot = *index_leaf;

      if (index_leaf->transaction_id.load() == tx_id) {
        validation_set_.push_back({index_leaf, tx_id});
        return snapshot;
      }
    }
  };

  /**
   * @brief Copy a DataItem using Silo's double-TID stable-read loop.
   *
   * @details Unlike Read(), this does not append to validation_set_. It is only
   * for non-unique secondary-index write seeds whose commit result is later
   * installed by merging recorded Add/Remove deltas under the write lock.
   */
  const DataItem ReadUnvalidated(const std::string_view,
                                 DataItem* index_leaf) final override {
    assert(index_leaf != nullptr);

    DataItem snapshot;
    for (;;) {
      auto tx_id = index_leaf->transaction_id.load();

      if (tx_id.tid & 1u) {
        _mm_pause();
        continue;
      }

      snapshot = *index_leaf;

      if (index_leaf->transaction_id.load() == tx_id) {
        return snapshot;
      }
    }
  };

  // See concurrency_control_base.h for why ReadDirect exists (Scan perf).
  // TID double-check protocol ensures the returned pointer is consistent.
  //
  // FIXME: ReadDirect is a Scan-specific lightweight path that registers
  // into validation_set_ without creating a Snapshot in read_set_. This
  // works for research purposes but breaks LineairDB's CC-switchable
  // design, as validation_set_ is Silo-specific internal state.
  //
  // The root issue is Snapshot cost: 384B + heap-copied row data per entry.
  // Under scan-heavy workloads, this doesn't scale: memory allocation cost
  // grows with client count and scan size, eventually causing OOM.
  //
  // Possible directions:
  // - Scan-mode Snapshot that skips data_item_copy (validation-only)
  // - Thread-local pooled allocator for Snapshot / DataBuffer
  // - Decouple validation tracking from read_set_ at the CC interface level
  std::pair<const std::byte*, size_t> ReadDirect(
      const std::string_view, DataItem* index_leaf,
      TransactionId& out_tid) final override {
    assert(index_leaf != nullptr);

    for (;;) {
      // Step 1: Load TID. Writers set the lock bit (LSB) before modifying data.
      auto tx_id = index_leaf->transaction_id.load();

      // Step 2: If lock bit is set, a writer holds this record. Spin until released.
      if (tx_id.tid & 1u) {
        _mm_pause();
        continue;
      }

      // Step 3: Read value pointer + size from DataItem buffer (no memcpy).
      // Primary rows never use the secondary posting-list pointer, so keep
      // this hot path to the size-only liveness check.
      const bool live = index_leaf->IsPrimaryInitialized();
      const std::byte* val;
      size_t sz;
      if (index_leaf->buffer.is_pax()) {
        // PAX rows have no contiguous bytes: gather into a thread-local
        // scratch. The callback consumes the pointer before the next
        // ReadDirect on this thread; a torn gather is rejected by the TID
        // re-check in Step 4 (same discipline as the zero-copy pointer).
        thread_local std::vector<std::byte> pax_scratch;
        sz = index_leaf->buffer.size;
        if (live && sz > 0) {
          if (pax_scratch.size() < sz) pax_scratch.resize(sz);
          index_leaf->buffer.GatherInto(pax_scratch.data());
          val = pax_scratch.data();
        } else {
          val = nullptr;
          sz = 0;
        }
      } else {
        val = index_leaf->buffer.value;
        sz = index_leaf->buffer.size;
      }

      // Step 4: Re-check TID. If unchanged, no concurrent writer modified the
      // data between Step 1 and Step 3, so the pointer is safe to return.
      if (index_leaf->transaction_id.load() == tx_id) {
        validation_set_.push_back({index_leaf, tx_id});
        out_tid = tx_id;
        return live ? std::pair<const std::byte*, size_t>{val, sz}
                    : std::pair<const std::byte*, size_t>{nullptr, 0};
      }
      // TID changed → a writer intervened. Retry from Step 1.
    }
  };

  void Write(const std::string_view, const std::byte* const, const size_t,
             DataItem*) final override{};
  void Abort() final override{};
  bool Precommit(bool need_to_checkpoint) final override {
    /** Sorting write set to prevent deadlock **/
    std::sort(tx_ref_.write_set_ref_.begin(), tx_ref_.write_set_ref_.end(),
              Snapshot::Compare);

    if constexpr (EnableNWR) {
      // An insert has to see the key it claimed, which omitting the write
      // would skip.
      if (!IsReadOnly() && !HasInsert() && IsOmittable()) {
        // NWR's omittable analysis orders versions but says nothing about
        // phantoms. Run deferred phantom validation here too; otherwise a
        // Masstree scan followed by an omittable commit can miss a
        // concurrent structural change in the scanned range.
        if (pre_commit_validator_ && !pre_commit_validator_()) {
          return false;
        }
        // we can safely clear writeset since all versions x_j in writeset_j are
        // omittable.
        tx_ref_.write_set_ref_.clear();
        return true;
      } else {
        // Preemptive abort: if anti_dependency validation of omittable version
        // order has failed, it is meaningless to acquire exclusive lockings
        // since the subsequent validation of Silo's version order will also
        // fail.
        if (nwr_validation_result_ == NWRValidationResult::ANTI_DEPENDENCY) {
          return false;
        }
      }
    }

    /** Acquire Lock **/
    std::vector<DataItem*> locked_items;
    locked_items.reserve(tx_ref_.write_set_ref_.size());
    auto unlock_locked_items = [&]() {
      for (auto* locked_item : locked_items) {
        auto current = locked_item->transaction_id.load();
        if (current.tid & 1u) {
          current.tid--;
          locked_item->transaction_id.store(current);
        }
      }
    };

    for (auto& snapshot : tx_ref_.write_set_ref_) {
      auto* item = snapshot.index_cache;
      assert(item != nullptr);
      __builtin_prefetch(item, 1, 3);

      for (;;) {
        auto current = item->transaction_id.load();
        if (current.tid & 1llu) {
          _mm_pause();
          continue;
        }
        auto desired = current;
        desired.tid |= 1llu;
        bool lock_acquired =
            item->transaction_id.compare_exchange_weak(current, desired);
        if (lock_acquired) {
          locked_items.push_back(item);
          snapshot.data_item_copy.transaction_id.store(desired);
          DataItem* attached_item = nullptr;
          bool checked_attachment = false;
          if (snapshot.index_name.empty() && snapshot.pi_ref != nullptr) {
            attached_item = snapshot.pi_ref->Get(snapshot.key);
            checked_attachment = true;
          } else if (!snapshot.index_name.empty() &&
                     snapshot.si_ref != nullptr) {
            attached_item = snapshot.si_ref->Get(snapshot.key);
            checked_attachment = true;
          }
          if (checked_attachment && attached_item != item) {
            unlock_locked_items();
            return false;
          }
          // Validate own-locked reads against the read-time TID with our lock
          // bit set. ReadDirect can duplicate entries, so update all matches.
          for (auto& read_item : validation_set_) {
            if (read_item.item_p_cache == item) {
              read_item.transaction_id.tid |= 1llu;
            }
          }
          break;
        }
      }
    }
    if (need_to_checkpoint) {
      for (auto& snapshot : tx_ref_.write_set_ref_) {
        snapshot.index_cache->CopyLiveVersionToStableVersion();
      }
    }

    /** Update Metadata for NWR **/
    if constexpr (EnableNWR) {
      UpdatePivotObjects();
    }

    // CompilerFence();
    tx_ref_.epoch_framework_ref_.MakeMeOffline();
    tx_ref_.epoch_framework_ref_.MakeMeOnline();
    // CompilerFence();

    /** Validation Phase **/
    if (!AntiDependencyValidation()) {
      // if validation failed, unlock all objects
      for (auto& snapshot : tx_ref_.write_set_ref_) {
        auto current = snapshot.index_cache->transaction_id.load();
        current.tid--;
        snapshot.index_cache->transaction_id.store(current);
      }
      return false;
    }

    // Deferred phantom validation (Masstree). Runs under write locks and
    // after AntiDepValidation, so concurrent leaf-version drift happening-
    // before this tx's serial point is observed. PL leaves the validator
    // unset; Masstree installs one in Transaction::Impl::Precommit.
    if (pre_commit_validator_ && !pre_commit_validator_()) {
      for (auto& snapshot : tx_ref_.write_set_ref_) {
        auto current = snapshot.index_cache->transaction_id.load();
        current.tid--;
        snapshot.index_cache->transaction_id.store(current);
      }
      return false;
    }

    // A write that claimed a free key must still find it free. The entry is
    // the one the write resolved and the lock loop proved is attached to the
    // key, so no reinsert of the slot can hide a row from this check.
    for (const auto& snapshot : tx_ref_.write_set_ref_) {
      if (!snapshot.is_insert) continue;
      // Only primary writes may carry the flag: SI liveness is not
      // IsPrimaryInitialized().
      assert(snapshot.index_name.empty());
      if (!snapshot.index_cache->IsPrimaryInitialized()) continue;
      aborted_by_duplicate_key_ = true;
      for (auto& locked : tx_ref_.write_set_ref_) {
        auto current = locked.index_cache->transaction_id.load();
        current.tid--;
        locked.index_cache->transaction_id.store(current);
      }
      return false;
    }

    /** Buffer Update **/
    //
    // Deletes install tombstones. Physical removal is deferred to the
    // epoch reaper so same-key reinserts reuse the slot and preserve the
    // slot's monotonic TID chain.
    //
    // Tags the install region with the commit epoch so the PAX
    // before-image capture can label its entries.
    Pax::ScopedCommitEpoch commit_epoch_scope(
        tx_ref_.epoch_framework_ref_.GetMyThreadLocalEpoch());
    size_t installed = 0;
    for (auto& snapshot : tx_ref_.write_set_ref_) {
      if (installed > 0) {
        LINEAIRDB_DEBUG_SYNC("silo_commit.between_row_installs");
      }
      ++installed;
      if (!snapshot.index_name.empty() &&
          !snapshot.secondary_index_deltas.empty() &&
          !snapshot.index_type.IsUnique()) {
        // Non-unique SI writes commute as Add/Remove deltas. Apply them to
        // the locked current PK-list instead of overwriting with a stale copy.
        for (const auto& delta : snapshot.secondary_index_deltas) {
          const auto* primary_key =
              reinterpret_cast<const std::byte*>(delta.primary_key.data());
          switch (delta.op) {
            case SecondaryIndexOp::Add:
              snapshot.index_cache->AddSecondaryIndexValue(
                  primary_key, delta.primary_key.size());
              break;
            case SecondaryIndexOp::Remove:
              snapshot.index_cache->RemoveSecondaryIndexValue(
                  primary_key, delta.primary_key.size());
              break;
            case SecondaryIndexOp::None:
            case SecondaryIndexOp::Full:
              assert(false);
              break;
          }
        }
        snapshot.data_item_copy = *snapshot.index_cache;
        continue;
      }
      *snapshot.index_cache = snapshot.data_item_copy;
    }

    return true;
  };

  void PostProcessing(TxStatus status) final override {
    if (status == TxStatus::Committed) {
      if constexpr (EnableNWR) {
        if (nwr_validation_result_ == NWRValidationResult::ACYCLIC) {
          return;
        }
      }

      const EpochNumber current_epoch =
          tx_ref_.epoch_framework_ref_.GetMyThreadLocalEpoch();

      /** Unlock **/
      for (auto& snapshot : tx_ref_.write_set_ref_) {
        auto* item = snapshot.index_cache;
        auto current_tid = snapshot.data_item_copy.transaction_id.load();
        EpochNumber written_in_epoch = current_tid.epoch;

        TransactionId unlocked_id;
        if (current_epoch != written_in_epoch) {
          unlocked_id = {current_epoch, 2};
        } else {
          unlocked_id = {current_epoch, current_tid.tid + 1};
        }
        item->transaction_id.store(unlocked_id);
        snapshot.data_item_copy.transaction_id.store(unlocked_id);
        if (tx_ref_.register_deferred_purge_) {
          tx_ref_.register_deferred_purge_(snapshot, unlocked_id);
        }
      }
    }
  }

  bool ObservedReadsStillValid() const final {
    for (const auto& validation_item : validation_set_) {
      if (validation_item.item_p_cache->transaction_id.load() !=
          validation_item.transaction_id) {
        return false;
      }
    }
    return true;
  }

 private:
  bool HasInsert() {
    for (const auto& snapshot : tx_ref_.write_set_ref_) {
      if (snapshot.is_insert) return true;
    }
    return false;
  }

  bool AntiDependencyValidation() {
    for (auto& validation_item : validation_set_) {
      auto* item = validation_item.item_p_cache;
      auto tx_id = item->transaction_id.load();
      if (tx_id != validation_item.transaction_id) {
        return false;
      }
    }
    return true;
  }

  bool IsOmittable() {
    // Brief: we now just collect and snapshot the pivot version objects.
    // Explanation: we first generate a version order << from the pivot
    // version objects for each data item. Let t_j be this transaction. A
    // pivot version object for x holds the pivot version x_pv, which is the
    // landmark for ordering x_j in the version order for x: for all x_j in
    // writeset_j, x_j < x_pv and there does not exist x_k such that x_j < x_k
    // < x_pv. In other words, the pivot version x_pv is just after version of
    // x_j, __in the generated version order <<__. When << fails to validate
    // the correctness, Silo generate the another version order which includes
    // x_pv < x_j by using exclusive locking.

    {  // snapshot the pivot version objects from write_set
      for (auto& snapshot : tx_ref_.write_set_ref_) {
        auto* value_ptr = snapshot.index_cache;
        assert(value_ptr != nullptr);

        const auto pivot_object = value_ptr->pivot_object.load();
        const PivotObjectSnapshot pv_snapshot = {value_ptr, pivot_object,
                                                 PivotObjectSnapshot::WRITESET};
        pivot_object_snapshots_.emplace_back(pv_snapshot);
      }
    }
    {  // snapshot the pivot version objects
      // from read_set
      for (auto& snapshot : tx_ref_.read_set_ref_) {
        auto* value_ptr = snapshot.index_cache;
        assert(value_ptr != nullptr);
        const auto pivot_object = value_ptr->pivot_object.load();
        const PivotObjectSnapshot pv_snapshot = {value_ptr, pivot_object,
                                                 PivotObjectSnapshot::READSET};
        pivot_object_snapshots_.emplace_back(pv_snapshot);
      }
    }

    // We now validate Linearizability.
    // In short, linearizability prohibits the ordering of version orders
    // among non-concurrent transactions. To validate the concurrency of
    // transactions, we use epoch: transactions in the same epoch are
    // committed at the same time, and thus they are in concurrent, and thus
    // any version order for these transactions are valid for linearizability.
    const EpochNumber current_epoch =
        tx_ref_.epoch_framework_ref_.GetMyThreadLocalEpoch();
    for (auto& pivot_object : pivot_object_snapshots_) {
      if (pivot_object.set_type == PivotObjectSnapshot::READSET) continue;
      const EpochNumber epoch = pivot_object.pv_snapshot.versions.epoch;
      if (epoch != current_epoch) {
        nwr_validation_result_ = NWRValidationResult::LINEARIZABILITY;
        return false;
      }
    }

    // Next, we prepare validations of serializability.
    // In validation phase, we must compare the values between t_j's
    // read/write sets and pivot version object for each x in writeset_j. To
    // this end efficiently, we squash the version numbers of read/write set
    // into PivotObject.
    my_pivot_object_.versions.epoch = current_epoch;
    {  // make t_j's squashed read/write set

      // MergedRS
      for (auto& snapshot : tx_ref_.read_set_ref_) {
        const auto value_ptr = snapshot.index_cache;
        auto tid = snapshot.data_item_copy.transaction_id.load();
        assert(value_ptr != nullptr);

        // Store the version number x_k read by t_j.
        // When x_k has written in the different (not the current) epoch,
        // version x_k in this merged RS must be less than all versions
        // written in the current epoch, and thus we store 1 as the oldest
        // version for all epochs, instead of actual value of 64-bits version
        // representation.
        if (tid.epoch == current_epoch) {
          my_pivot_object_.msets.rset.PutHigherside(value_ptr, tid.tid);
        } else {
          my_pivot_object_.msets.rset.PutHigherside(value_ptr, 1);
        }
      }

      // MergedWS
      for (auto& pivot_object : pivot_object_snapshots_) {
        if (pivot_object.set_type != PivotObjectSnapshot::WRITESET) continue;
        const auto* value_ptr = pivot_object.item_p_cache;
        uint32_t tk = pivot_object.pv_snapshot.versions.target_id;
        assert(pivot_object.pv_snapshot.versions.epoch == current_epoch);

        my_pivot_object_.msets.wset.PutHigherside(value_ptr, tk);
      }
    }

    // Now we validate the version order << given by the pivot version
    // objects.
    // Validate Serializability 1.
    // Successors_j := {|Tk| wk(xk) in H and xj << xk and rg(xk) in H }
    // if there exists t_k such that t_k in successors_j and t_k -> ...,
    // -> t_j, there exists dependency cycle in MVSG.
    for (auto& pivot_object : pivot_object_snapshots_) {
      if (pivot_object.set_type != PivotObjectSnapshot::WRITESET) continue;
      auto& tj = my_pivot_object_;
      auto& tk = pivot_object.pv_snapshot;

      auto result = tk.IsReachableInto(tj);
      if (result != NWRValidationResult::ACYCLIC) {
        nwr_validation_result_ = result;
        return false;
      }
    }
    // Validate serializability 2:
    // Overwriters_j := {|Tg| rj(xk) in H and (xk = rg or xk << xg))}
    // if there exists t_k in overwriters_j such that Tk -> ..., -> Tj,
    // there exists dependency cycle in MVSG.
    // We adopt the same strategy with the baseline (Silo): if there exists
    // newer version of x, then we simply regard MVSG may be not acyclic.
    if (!AntiDependencyValidation()) {
      nwr_validation_result_ = NWRValidationResult::ANTI_DEPENDENCY;
      return false;
    }

    if (RequiresLockBasedInstall()) {
      // Delta writes must merge with the current locked PK-list; they are not
      // safe to clear through NWR's omitted-version path.
      return false;
    }

    // We must updating mRS and mWS for each data item in read/write set,
    // to ensure serializability between this transaction and concurrent NWR
    // procedures. This updating need to execute CAS-loop.
    bool all_cas_succeed = true;
    for (auto& snapshot : pivot_object_snapshots_) {
      auto* data_item_p = snapshot.item_p_cache;
      auto& atomic_ref = data_item_p->pivot_object;
      auto& old_snapshot = snapshot.pv_snapshot;
      auto new_snapshot = old_snapshot;

      new_snapshot.msets.rset =
          new_snapshot.msets.rset.Merge(my_pivot_object_.msets.rset);
      new_snapshot.msets.wset =
          new_snapshot.msets.wset.Merge(my_pivot_object_.msets.wset);

      if (new_snapshot.msets == old_snapshot.msets) continue;

      all_cas_succeed =
          atomic_ref.compare_exchange_weak(old_snapshot, new_snapshot);
      if (!all_cas_succeed) break;
    }

    // Unfortunately some CAS operations have failed.
    // but Anti-dependency validation may still pass this transaction and thus
    // we retry all procedure: generate version order, taking snapshots, do
    // validations for NWR, and finally trying to CAS.
    if (!all_cas_succeed) {
      return IsOmittable();
    }

    // Fortunately we can safely omit this transaction.
    nwr_validation_result_ = NWRValidationResult::ACYCLIC;
    return true;
  }

  /**
   * @brief Return true when a write must install through the lock-based path.
   *
   * @details NWR's omitted-version path can clear the write set without running
   * the buffer install loop. Non-unique secondary-index deltas must reach that
   * loop so they can merge with the locked current PK-list.
   */
  bool RequiresLockBasedInstall() const {
    for (const auto& snapshot : tx_ref_.write_set_ref_) {
      if (!snapshot.index_name.empty() &&
          !snapshot.secondary_index_deltas.empty() &&
          !snapshot.index_type.IsUnique()) {
        return true;
      }
    }
    return false;
  }

  /**
   * @brief
   * Update the metadata (the pivot objects) for each data item in readset or
   * writeset, for the NWR-validation of the other transactions. This method
   * must be invoked before Anti-dependency validation.
   * @details
   * In SiloNWR, a transaction is committed by CAS-loop if the NWR-validation is
   * succeed. Otherwise, it runs on lock-base as with the Silo. Concurrency
   * control between these two different types of transactions, CAS-based and
   * Lock-based, is a difficult task since the NWR-validation does not check the
   * state of lockings. The following proposition helps us to solve this
   * problem:
   *   - Let t_cas be a CAS-based committed transaction.
   *   - Let t_lock be a lock-based committed transaction.
   *   - If t_lock updates the pivot version objects "before" its
   *     anti_dependency_validation, they do not violate serializability.
   * Proof (sketch):
   * To prove by contradiction, suppose that an MVSG of t_cas and t_lock depict
   * a cycle. That is, there is a transitive path t_cas -> t_lock -> t_cas in
   * the graph. Note that t_cas performs CAS into the pivot version object of
   * some pivot version. Let x_pv be the version such that there exists
   * w_cas(x_cas) and x_cas < x_pv. If t_lock also reads or writes the same data
   * item x and commits, then the accessed version x_k is always a version
   * greater than x_pv.
   * That is, t_cas -> t_lock consists of a single data item, but
   * in order to draw t_lock -> t_cas, one more data item `y` must be assumed.
   * Here we show the exaustive two cases as the followings:
   *   -# t_lock (wr)-> t_cas:
   *     - i.e., t_cas reads some newer version y_k such that y_lock < y_k
   *     - If c_cas precedes c_lock, it does not holds since t_lock holds the
   *       exclusive lock of y until c_lock and c_cas cannot read y_k.
   *     - Otherwise, t_cas aborts since the NWR-validation tells the edge
   *       t_cas -> t_lock.
   *   -# t_lock <<(rw, ww)-> t_cas:
   *      - i.e., t_cas writes some newer version y_cas, such that:
   *        -# y_lock < y_cas < y_pv
   *        -# y_k < y_cas < y_pv and r_lock(y_k) for some y_k
   *           - If c_cas precedes w_lock(y_lock), y_pv cannot be exist.
   *           - Otherwise, t_cas aborts by the NWR-validation.
   */
  void UpdatePivotObjects() {
    // Let t_j be this transaction.
    // Now update the pivot objects for all data items in read/write set of
    // t_j. Updating procedure will be completed by atomic::compare_exchange
    // operation into each data item with my_pivot_object.
    // We assume that this method is invoked from lock-based (not NWR)
    // protocol.
    assert(nwr_validation_result_ != NWRValidationResult::ACYCLIC);

    // Re-build my pivot object
    const EpochNumber current_epoch =
        tx_ref_.epoch_framework_ref_.GetMyThreadLocalEpoch();
    my_pivot_object_.versions.epoch = current_epoch;
    {  // make t_j's squashed read/write set

      // MergedRS
      for (auto& snapshot : tx_ref_.read_set_ref_) {
        const auto* value_ptr = snapshot.index_cache;
        auto tid = snapshot.data_item_copy.transaction_id.load();
        assert(value_ptr != nullptr);
        if (tid.epoch == current_epoch) {
          my_pivot_object_.msets.rset.PutLowerside(value_ptr, tid.tid);
        } else {
          my_pivot_object_.msets.rset.PutLowerside(value_ptr, 1);
        }
      }

      // MergedWS
      for (auto& snapshot : tx_ref_.write_set_ref_) {
        const auto* value_ptr = snapshot.index_cache;
        assert(value_ptr != nullptr);
        auto tid = snapshot.data_item_copy.transaction_id.load();
        assert(tid.tid & 1llu);  // is locked

        auto new_version = tid.tid;
        if (tid.epoch == current_epoch) {
          new_version += 1;  // unlocked version
        } else {
          new_version = 2;  // the first unlocked version in this epoch
        }

        my_pivot_object_.msets.wset.PutHigherside(value_ptr, new_version);
      }
    }

    // Updating mRS and mWS
    for (auto& snapshot : pivot_object_snapshots_) {
      auto* data_item_p = snapshot.item_p_cache;
      auto& atomic_ref = data_item_p->pivot_object;
      auto old_snapshot = atomic_ref.load();

      // If this transaction performs the first-blind write into the data item
      // in this epoch, update the pivot version.
      if (old_snapshot.versions.epoch != current_epoch &&
          snapshot.set_type == PivotObjectSnapshot::WRITESET) {
        Snapshot* ws_entry_for_this_snapshot = nullptr;
        for (auto& ws_entry : tx_ref_.write_set_ref_) {
          if (ws_entry.index_cache != snapshot.item_p_cache) continue;
          if (ws_entry.is_read_modify_write) break;

          ws_entry_for_this_snapshot = &ws_entry;
        }
        if (ws_entry_for_this_snapshot != nullptr) {
          // It is the first blind write into the data item in this epoch
          auto new_snapshot = my_pivot_object_;
          assert(new_snapshot.versions.epoch == current_epoch);
          new_snapshot.versions.target_id =
              ws_entry_for_this_snapshot->data_item_copy.transaction_id.load()
                  .tid;
          data_item_p->pivot_object.store(new_snapshot);
          continue;
        }
      }

      bool cas_success = false;
      while (!cas_success) {
        old_snapshot = data_item_p->pivot_object.load();
        auto new_snapshot = old_snapshot;
        auto new_rset =
            new_snapshot.msets.rset.Merge(my_pivot_object_.msets.rset);
        auto new_wset =
            new_snapshot.msets.wset.Merge(my_pivot_object_.msets.wset);
        new_snapshot.msets.rset = new_rset;
        new_snapshot.msets.wset = new_wset;

        cas_success =
            atomic_ref.compare_exchange_weak(old_snapshot, new_snapshot);
      }
    }
  }
};

using SiloNWR = SiloNWRTyped<true>;
using Silo = SiloNWRTyped<false>;

}  // namespace ConcurrencyControl
}  // namespace LineairDB
#endif /* LINEAIRDB_SILO_NWR_H */
