#ifndef LINEAIRDB_STATELESS_PACKED_TRANSACTION_ID_HPP
#define LINEAIRDB_STATELESS_PACKED_TRANSACTION_ID_HPP

#include <cstdint>

#include "types/definitions.h"
#include "types/transaction_id.hpp"

namespace LineairDB {
namespace Stateless {

/**
 * @brief Pack a {epoch, tid} pair into one uint64_t so it can travel over
 * the stateless RPC as an opaque version token.
 */
inline uint64_t PackTransactionId(const TransactionId& tid) {
  return (static_cast<uint64_t>(tid.epoch) << 32) |
         static_cast<uint64_t>(tid.tid);
}

/**
 * @brief Inverse of PackTransactionId.
 *
 * The stateless API hands the packed value back through ValidateAndCommit,
 * which unpacks it to compare with the live TransactionId on the DataItem.
 */
inline TransactionId UnpackTransactionId(uint64_t packed) {
  return {static_cast<EpochNumber>(packed >> 32),
          static_cast<uint32_t>(packed & 0xffffffffu)};
}

}  // namespace Stateless
}  // namespace LineairDB

#endif  // LINEAIRDB_STATELESS_PACKED_TRANSACTION_ID_HPP
