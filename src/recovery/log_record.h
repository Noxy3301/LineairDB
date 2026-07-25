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
 * One committed transaction's write set, as it is persisted. The epoch is the
 * transaction's commit epoch; the WAL groups records by it, and recovery
 * replays a record only when the frame carrying it is complete.
 *
 * Lives at namespace scope rather than inside Logger so that the WAL codec can
 * name it without depending on the logger interface. Logger keeps aliases for
 * the nested names its existing callers use.
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
