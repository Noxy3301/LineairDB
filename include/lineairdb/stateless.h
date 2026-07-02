#ifndef LINEAIRDB_STATELESS_H
#define LINEAIRDB_STATELESS_H

#include <cstdint>
#include <string>
#include <vector>

namespace LineairDB {

/**
 * @file stateless.h
 * @brief Types for the stateless read / validate-and-commit API.
 */

// ---------------------------------------------------------------------
// Read and scan outcomes: pure observations.
// ---------------------------------------------------------------------

/**
 * @brief Outcome of a single Database::StatelessRead.
 *
 * `tid` is the packed (epoch:32 | tid:32) version observed at read time.
 * Resubmit the same `tid` through ValidateAndCommit inside an
 * ExternalReadEntry to assert that the row did not move before commit.
 */
struct StatelessReadResult {
  bool found = false;   ///< True when the key existed and was non-empty.
  std::string value;    ///< Row payload, valid only when @ref found is true.
  uint64_t tid = 0;     ///< Packed (epoch | tid) version observed at read time.
};

/**
 * @brief One row from a primary-index range scan.
 */
struct StatelessScanRow {
  std::string key;
  std::string value;
  uint64_t tid = 0;     ///< Packed version observed for this row.
  bool found = false;   ///< Tombstones are normally filtered out before this struct is produced.
};

/**
 * @brief One row from a secondary-index range scan.
 *
 * `secondary_key` is the indexed key, `primary_key` is the base-table key
 * reached through it, and `value` is the corresponding base-table row.
 */
struct StatelessSecondaryScanRow {
  std::string secondary_key;
  std::string primary_key;
  std::string value;
  uint64_t tid = 0;     ///< Packed version of the base row.
  bool found = false;
};

/**
 * @brief Outcome of Database::StatelessRangeScan.
 *
 * `ok` separates a genuine empty result from a Masstree retry that gave up
 * or a missing table. On `ok == false` the caller should abort the logical
 * transaction.
 */
struct StatelessRangeScanResult {
  bool ok = false;
  std::vector<StatelessScanRow> rows;
};

/**
 * @brief One row reference from a PAX ref-scan: the row's payload is not
 * materialized; the caller reads the cells it needs straight from the PAX
 * group and re-checks `item TID == tid` afterwards (torn-read rejection).
 *
 * `group`/`slot` stay valid for the lifetime of the store; `item` stays
 * valid until the caller releases its Masstree epoch (RPC boundary).
 */
struct StatelessPaxRefRow {
  std::string key;
  const void* group = nullptr;  ///< Pax::PaxGroup*, opaque to keep this header light.
  uint32_t slot = 0;
  uint32_t row_size = 0;        ///< Payload byte size observed at scan time.
  uint64_t tid = 0;             ///< Packed version observed at scan time.
  const void* item = nullptr;   ///< DataItem*, for the post-read TID re-check.
};

/**
 * @brief Outcome of Database::StatelessPaxRefScan.
 *
 * `ok == false` when the table is missing, has no PAX store, contains any
 * heap-fallback row (overflow), or the index scan gave up — the caller must
 * use the materializing scan instead.
 */
struct StatelessPaxRefScanResult {
  bool ok = false;
  std::vector<StatelessPaxRefRow> rows;
};

/**
 * @brief Re-read the packed TID behind a PAX ref row (post-cell-read check).
 *
 * Returns a value different from `row.tid` (or with the lock bit set) when
 * a writer touched the row after the scan observed it — the caller must
 * re-read that row through a validated path (e.g. StatelessRead).
 */
uint64_t PaxRefCurrentTid(const StatelessPaxRefRow& row);

/**
 * @brief Outcome of Database::StatelessSecondaryRangeScan.
 */
struct StatelessSecondaryRangeScanResult {
  bool ok = false;
  std::vector<StatelessSecondaryScanRow> rows;
};

// ---------------------------------------------------------------------
// ValidateAndCommit inputs: the caller assembles these from the
// observations above.
// ---------------------------------------------------------------------

/**
 * @brief Point read to revalidate at commit.
 *
 * `tid` and `found` come from an earlier read or scan row. ValidateAndCommit
 * aborts when the row's TID moved; `found == false` asserts the key was
 * absent and aborts when a row appeared.
 */
struct ExternalReadEntry {
  std::string table_name;
  std::string key;
  uint64_t tid = 0;
  bool found = false;
};

/**
 * @brief Row write or delete to install during ValidateAndCommit.
 *
 * When `is_delete` is true, `value` is ignored and the row is removed.
 */
struct ExternalWriteEntry {
  std::string table_name;
  std::string key;
  std::string value;
  bool is_delete = false;
};

/**
 * @brief Secondary-index add or remove to install during ValidateAndCommit.
 */
struct ExternalSecondaryIndexEntry {
  std::string table_name;
  std::string index_name;
  std::string secondary_key;
  std::string primary_key;
  bool is_delete = false;
};

/**
 * @brief Range read to revalidate at commit.
 *
 * Assemble it from the scan request and the returned rows: the bounds
 * describe the scan to re-run, `result_keys` (plus `result_primary_keys`
 * on a secondary index) is the key set the scan returned, in scan order.
 * ValidateAndCommit replays the scan and aborts when the key set changed.
 * Row TIDs are not part of this entry; register every returned row as an
 * ExternalReadEntry instead.
 */
struct ExternalRangeReadEntry {
  std::string table_name;
  std::string index_name;   ///< Empty marks a primary-index range.
  std::string start_key;    ///< Scan start (inclusive).
  std::string end_key;      ///< Scan end (exclusive). Must be non-empty.
  uint64_t row_limit = 0;   ///< Row cap applied during scan.
  bool reverse_scan = false;///< Scan direction.
  std::vector<std::string> result_keys;          ///< Observed key set.
  std::vector<std::string> result_primary_keys;  ///< Secondary index: paired primary keys.
};

}  // namespace LineairDB

#endif  // LINEAIRDB_STATELESS_H
