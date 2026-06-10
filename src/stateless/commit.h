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
 * @brief Run the Silo commit protocol for a transaction whose read and
 * write sets were assembled by the caller through the stateless API.
 *
 * @details
 * The payload arrives by value; nothing in it references server memory:
 *   - reads:       (key, observed TID, found)
 *   - writes:      (key, value | delete)
 *   - SI ops:      (secondary key, primary key, add | remove)
 *   - range reads: scan bounds plus the returned key list
 *
 * The protocol is Silo's commit protocol (paper §4.4), bracketed by an
 * epoch join and leave; [Helios] marks implementation additions over
 * the paper for by-key inputs and SQL UNIQUE semantics:
 *
 *   Resolve   R1  [Helios] map every key to its DataItem
 *             R2  [Helios] materialize blank slots for fresh write keys
 *             R3  [Helios] reject in-request UNIQUE duplicates
 *   Phase 1   1.1 [Silo]   lock the write set in address order
 *             1.2 [Silo]   re-read the global epoch (serialization point)
 *   Phase 2   2.1 [Silo]   exact reads: observed TIDs unmoved
 *             2.2 [Helios] ranges: replay the scans, compare key lists
 *                          (membership only; row TIDs are validated
 *                          at 2.1)
 *             2.3 [Helios] UNIQUE recheck after the lock wait
 *   Phase 3   3.1 [Silo]   install values; deletes become tombstones
 *             3.2 [Silo]   log snapshot before unlock (when logging)
 *             3.3 [Silo]   publish even TIDs stamped with the 1.2 epoch
 *             3.4 [Helios] hand slots left empty to the reaper for
 *                          deferred physical purge
 *             3.5 [Silo]   enqueue the log set, leave the epoch
 *
 * @note Read validation is logical: Phase 2 re-reads every key and
 * replays every scan, then requires the observed TIDs and the result
 * key lists to be unchanged. Silo instead guards ranges with Masstree
 * node versions — referred to as physical validation here — but a node
 * version is bound to a node pointer, and this server keeps no
 * per-transaction state that could pin such a pointer across the RPC
 * boundary, so its lifetime cannot be guaranteed.
 *
 * @param[out] abort_reason When non-null and the attempt aborts,
 * receives a short label naming the failed check, such as
 * `exact_read_tid_moved`, `primary_range_result_changed`, or
 * `unique_si_exists_after_lock`.
 * @return true when the transaction committed; false on abort, after
 * every lock this attempt acquired has been released.
 */
bool Commit(TableDictionary& tables, std::shared_mutex& schema_mutex,
            EpochFramework& epoch_framework, Index::Reaper& reaper,
            Recovery::Logger& logger, const Config& config,
            const std::vector<ExternalReadEntry>& reads,
            const std::vector<ExternalWriteEntry>& writes,
            const std::vector<ExternalSecondaryIndexEntry>& secondary_index_ops,
            const std::vector<ExternalRangeReadEntry>& range_reads,
            std::string* abort_reason);

}  // namespace Stateless
}  // namespace LineairDB

#endif  // LINEAIRDB_STATELESS_COMMIT_H
