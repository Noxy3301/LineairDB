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
 * @brief Range-scan the primary index and return rows plus validation
 *        tokens.
 *
 * Drives Index::Scan / Index::ScanReverse with a callback that, for each
 * hit, performs the same double-TID read used by Read. The Masstree scan
 * also fills a `NodeVersionEntry` vector covering every touched leaf;
 * those are returned as `range_versions` so ValidateAndCommit can run
 * `ValidatePhantoms` later. Tombstones encountered during the scan are
 * recorded as exact-key entries in `index_reads`, because a node-version
 * check alone misses a tombstone slot being reused without a tree-shape
 * change.
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
 * @brief Range-scan a secondary index and resolve each hit to its base
 *        row.
 *
 * For every secondary key in `[start_key, end_key)`, looks up its
 * `primary_keys()` and, for each one, performs the same double-TID base
 * read as Read. Each secondary slot's TID is also captured as an
 * `ExternalIndexValidationEntry` so SI rewrites at commit time are caught.
 * Like RangeScan, the Masstree-side node versions feed `range_versions`
 * for phantom validation, and `ok == false` is the abort signal.
 */
StatelessSecondaryRangeScanResult SecondaryRangeScan(
    TableDictionary& tables, std::shared_mutex& schema_mutex,
    std::string_view table_name, std::string_view index_name,
    std::string_view start_key, std::string_view end_key, uint64_t row_limit,
    bool reverse_scan);

}  // namespace Stateless
}  // namespace LineairDB

#endif  // LINEAIRDB_STATELESS_READ_H
