#ifndef LINEAIRDB_STATELESS_READ_H
#define LINEAIRDB_STATELESS_READ_H

#include <lineairdb/stateless.h>

#include <cstdint>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace LineairDB {

class TableDictionary;

/**
 * Read side of the stateless API: point reads and range scans that run
 * without opening a Transaction. The functions are stateless, so they take
 * the table dictionary and the schema mutex from the caller instead of
 * holding them. Every call takes a shared lock on the schema, resolves
 * index slots, and copies rows with the Silo-style stable read; the
 * returned packed TIDs are the read-set evidence the caller later submits
 * through ValidateAndCommit.
 */
namespace Stateless {

/**
 * @brief Read one row without opening a Transaction.
 *
 * Takes a shared lock on the schema, resolves the primary-index slot, and
 * performs a Silo-style double TID read on the DataItem: load the TID,
 * yield while the lock bit (LSB) is set, copy the value, then re-load the
 * TID and only return it if it has not moved. The caller keeps the
 * returned `tid` and submits it through ValidateAndCommit later.
 */
StatelessReadResult Read(TableDictionary& tables,
                         std::shared_mutex& schema_mutex,
                         std::string_view table_name, std::string_view key);

/**
 * @brief Read several rows in one call.
 *
 * Reuses Read per entry. Reads are independent, so this is purely a
 * transport optimization that lets a caller fold N point reads into one
 * RPC.
 */
std::vector<StatelessReadResult> BatchRead(
    TableDictionary& tables, std::shared_mutex& schema_mutex,
    const std::vector<std::pair<std::string, std::string>>& keys);

/**
 * @brief Range-scan the primary index and return the rows observed in
 *        the range.
 *
 * Drives Index::Scan / Index::ScanReverse with a callback that, for each
 * hit, performs the same double-TID read used by Read. The caller
 * assembles the commit-time ExternalRangeReadEntry from its own scan
 * arguments and the returned keys. Tombstones are skipped: key-list
 * validation catches any reuse of their slots without a per-entry TID.
 *
 * `ok` distinguishes a genuine empty result from a Masstree retry that
 * gave up. Callers should treat `!ok` as an abort signal.
 */
StatelessRangeScanResult RangeScan(TableDictionary& tables,
                                   std::shared_mutex& schema_mutex,
                                   std::string_view table_name,
                                   std::string_view start_key,
                                   std::string_view end_key,
                                   uint64_t row_limit, bool reverse_scan);

/**
 * @brief Range-scans the primary index and returns PAX cell references.
 *
 * @details The caller evaluates cells directly and re-checks each row TID
 * after reading. `ok == false` means the caller should use RangeScan instead.
 */
StatelessPaxRowRefScanResult PaxRowRefScan(
    TableDictionary& tables, std::shared_mutex& schema_mutex,
    std::string_view table_name, std::string_view start_key,
    std::string_view end_key, uint64_t row_limit, bool reverse_scan);

/**
 * @brief Range-scan a secondary index and resolve each hit to its base
 *        row.
 *
 * For every secondary key in `[start_key, end_key)`, pins its immutable
 * primary-key list and, for each key in the view, performs the same double-TID
 * base read as Read. The caller assembles the commit-time
 * ExternalRangeReadEntry from its own scan arguments and both returned
 * key lists. `ok == false` is the abort signal, as in RangeScan.
 */
StatelessSecondaryRangeScanResult SecondaryRangeScan(
    TableDictionary& tables, std::shared_mutex& schema_mutex,
    std::string_view table_name, std::string_view index_name,
    std::string_view start_key, std::string_view end_key, uint64_t row_limit,
    bool reverse_scan);

}  // namespace Stateless
}  // namespace LineairDB

#endif  // LINEAIRDB_STATELESS_READ_H
