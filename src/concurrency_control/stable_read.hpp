#ifndef LINEAIRDB_CONCURRENCY_CONTROL_STABLE_READ_HPP
#define LINEAIRDB_CONCURRENCY_CONTROL_STABLE_READ_HPP

#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <xmmintrin.h>

#include "types/data_item.hpp"
#include "types/transaction_id.hpp"

namespace LineairDB {
namespace ConcurrencyControl {

/**
 * @brief Silo-style stable read (tuple.h stable_read in the reference
 * implementation): load the TID, yield while the lock bit (LSB) is set,
 * copy the payload, then re-load the TID and retry until it has not moved.
 *
 * SiloNWR::Read and ReadDirect run the same loop inline; these helpers
 * serve callers that need the copy without a transaction. The returned TID
 * is the version the copy is consistent with.
 */

struct StableValue {
  bool found = false;
  std::string value;
  TransactionId tid;
};

struct StablePrimaryKeys {
  bool found = false;
  PackedPrimaryKeys::Ptr primary_keys;
  TransactionId tid;

  PackedPrimaryKeysView primary_keys_view() const {
    return PackedPrimaryKeysView(primary_keys);
  }

  std::vector<std::string> primary_keys_vector() const {
    std::vector<std::string> result;
    const auto view = primary_keys_view();
    result.reserve(view.size());
    for (std::string_view primary_key : view) {
      result.emplace_back(primary_key.data(), primary_key.size());
    }
    return result;
  }
};

/**
 * @brief Stable read of a base-row DataItem.
 *
 * `found` is false for tombstones and uninitialized slots.
 */
inline StableValue StableReadValue(const DataItem& item) {
  for (;;) {
    TransactionId tid = item.transaction_id.load();
    if (tid.tid & 1u) {
      _mm_pause();
      continue;
    }

    const bool found = item.IsPrimaryInitialized();
    std::string value;
    if (found) {
      if (item.buffer.is_pax()) {
        // Gather the row from its PAX strips; a torn gather (concurrent
        // install) is rejected by the TID re-check below, same as a torn
        // pointer copy would be.
        value.resize(item.size());
        item.buffer.GatherInto(reinterpret_cast<std::byte*>(value.data()));
      } else {
        value.assign(reinterpret_cast<const char*>(item.value()), item.size());
      }
    }

    if (item.transaction_id.load() == tid) {
      return {found, std::move(value), tid};
    }
  }
}

/**
 * @brief Stable read that materializes only the listed columns (0-based,
 * ascending) for PAX-resident rows, leaving the other fields as 1-byte
 * placeholders (full row shape, sparse payload). Heap rows return the full
 * row — the sparse form is an optimization, never a semantic change, so
 * mixed storage stays correct. Same double-TID torn-read discipline as
 * StableReadValue.
 */
inline StableValue StableReadValueSparse(const DataItem& item,
                                         const uint32_t* columns,
                                         size_t n_columns) {
  for (;;) {
    TransactionId tid = item.transaction_id.load();
    if (tid.tid & 1u) {
      _mm_pause();
      continue;
    }

    const bool found = item.IsPrimaryInitialized();
    std::string value;
    if (found) {
      if (item.buffer.is_pax()) {
        item.buffer.pax_group()->GatherRowSparse(item.buffer.pax_slot(),
                                                 columns, n_columns, value);
      } else {
        value.assign(reinterpret_cast<const char*>(item.value()), item.size());
      }
    }

    if (item.transaction_id.load() == tid) {
      return {found, std::move(value), tid};
    }
  }
}

/**
 * @brief Stable read of a secondary-index DataItem, pinning its immutable
 * primary-key list.
 *
 * `found` is false when the slot is uninitialized or the list is empty.
 */
inline StablePrimaryKeys StableReadPrimaryKeys(const DataItem& item) {
  for (;;) {
    TransactionId tid = item.transaction_id.load();
    if (tid.tid & 1u) {
      _mm_pause();
      continue;
    }

    auto primary_keys = std::atomic_load(&item.primary_keys_);
    const bool found = primary_keys && primary_keys->count != 0;

    if (item.transaction_id.load() == tid) {
      return {found, std::move(primary_keys), tid};
    }
  }
}

}  // namespace ConcurrencyControl
}  // namespace LineairDB

#endif  // LINEAIRDB_CONCURRENCY_CONTROL_STABLE_READ_HPP
