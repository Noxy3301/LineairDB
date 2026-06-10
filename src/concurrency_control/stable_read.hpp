#ifndef LINEAIRDB_CONCURRENCY_CONTROL_STABLE_READ_HPP
#define LINEAIRDB_CONCURRENCY_CONTROL_STABLE_READ_HPP

#include <string>
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
  std::vector<std::string> primary_keys;
  TransactionId tid;
};

/**
 * @brief Stable read of a base-row DataItem.
 *
 * `found` is false for tombstones (initialized but empty) and
 * uninitialized slots.
 */
inline StableValue StableReadValue(const DataItem& item) {
  for (;;) {
    TransactionId tid = item.transaction_id.load();
    if (tid.tid & 1u) {
      _mm_pause();
      continue;
    }

    const bool found = item.IsInitialized() && item.size() != 0;
    std::string value;
    if (found) {
      value.assign(reinterpret_cast<const char*>(item.value()), item.size());
    }

    if (item.transaction_id.load() == tid) {
      return {found, std::move(value), tid};
    }
  }
}

/**
 * @brief Stable read of a secondary-index DataItem, copying its
 * primary-key list.
 *
 * `found` is false when the slot is uninitialized or the list is empty.
 */
inline StablePrimaryKeys StableReadPrimaryKeys(const DataItem& item) {
  std::vector<std::string> primary_keys;
  for (;;) {
    TransactionId tid = item.transaction_id.load();
    if (tid.tid & 1u) {
      _mm_pause();
      continue;
    }

    const bool found = item.IsInitialized() && !item.primary_keys().empty();
    if (found) primary_keys = item.primary_keys();

    if (item.transaction_id.load() == tid) {
      return {found, std::move(primary_keys), tid};
    }
  }
}

}  // namespace ConcurrencyControl
}  // namespace LineairDB

#endif  // LINEAIRDB_CONCURRENCY_CONTROL_STABLE_READ_HPP
