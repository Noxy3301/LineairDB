#ifndef LINEAIRDB_STATELESS_COMMIT_H
#define LINEAIRDB_STATELESS_COMMIT_H

#include <lineairdb/config.h>
#include <lineairdb/stateless.h>

#include <shared_mutex>
#include <string>
#include <vector>

namespace LineairDB {

class TableDictionary;
class EpochFramework;

namespace Recovery {
class Logger;
}

namespace Index {
class Reaper;
}

namespace Stateless {

/**
 * @brief Run the full Silo-style commit attempt for a transaction whose
 * read and write sets were assembled by the caller through the stateless
 * API: resolve the supplied sets, lock the write set, validate the reads
 * against the live index, install the writes, publish the new TIDs, and
 * enqueue the log record.
 *
 * Returns true when the transaction committed. On abort, `abort_reason`
 * (when given) carries a short token naming the failed check. The detailed
 * step narration lives with the implementation in commit.cpp.
 */
bool Commit(TableDictionary& tables, std::shared_mutex& schema_mutex,
            EpochFramework& epoch_framework, Index::Reaper& reaper,
            Recovery::Logger& logger, const Config& config,
            const std::vector<ExternalReadEntry>& reads,
            const std::vector<ExternalWriteEntry>& writes,
            const std::vector<ExternalSecondaryIndexEntry>& secondary_index_ops,
            const std::vector<ExternalRangeValidationEntry>& range_reads,
            const std::vector<ExternalIndexValidationEntry>& index_reads,
            std::string* abort_reason);

}  // namespace Stateless
}  // namespace LineairDB

#endif  // LINEAIRDB_STATELESS_COMMIT_H
