#ifndef LINEAIRDB_RECOVERY_LOG_RECORD_H
#define LINEAIRDB_RECOVERY_LOG_RECORD_H

#include <cstdint>
#include <msgpack.hpp>
#include <string>
#include <vector>

#include "types/definitions.h"
#include "types/transaction_id.hpp"

namespace LineairDB {
namespace Recovery {

/**
 * @brief An epoch-tagged group of key-value writes, as it is persisted.
 *
 * @details The logger emits one per committed transaction whose write set is
 * non-empty, tagged with its commit epoch; the checkpoint writer packs a
 * whole-database snapshot into one.
 *
 * @note Lives at namespace scope rather than inside Logger so that
 * persistence code can name it without depending on the logger interface.
 * Logger keeps aliases for the nested names its existing callers use.
 */
struct LogRecord {
  struct KeyValuePair {
    std::string key;
    std::string buffer;
    TransactionId tid;
    std::string table_name;
    std::string index_name;
    uint32_t index_type = 0;
    std::vector<std::string> primary_keys;
    uint8_t secondary_op = 0;
    std::string secondary_primary_key;
    MSGPACK_DEFINE(key, buffer, tid, table_name, index_name, index_type,
                   primary_keys, secondary_op, secondary_primary_key);
  };

  EpochNumber epoch;
  std::vector<KeyValuePair> key_value_pairs;
  MSGPACK_DEFINE(epoch, key_value_pairs);

  LogRecord() : epoch(0), key_value_pairs(0) {}
};

using LogRecords = std::vector<LogRecord>;

}  // namespace Recovery
}  // namespace LineairDB

#endif /* LINEAIRDB_RECOVERY_LOG_RECORD_H */
