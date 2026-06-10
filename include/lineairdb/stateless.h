#ifndef LINEAIRDB_STATELESS_H
#define LINEAIRDB_STATELESS_H

#include <cstdint>
#include <string>
#include <vector>

namespace LineairDB {

/// @file stateless.h
/// @brief Types for the stateless read / validate-and-commit API.
///
/// The conventional Transaction path stores its read set, write set, and
/// node-version set on a per-thread workspace inside LineairDB. The stateless
/// API does not: each call returns its observations by value (row value,
/// packed TID, scanned key list) to the caller. The caller carries them
/// across independent RPCs and resubmits them through
/// Database::ValidateAndCommit when the logical transaction is ready to
/// commit. Because no state is retained on the database side, calls can be
/// spread across any thread.

/// Outcome of a single Database::StatelessRead.
///
/// `tid` is the packed (epoch:32 | tid:32) version observed at read time.
/// Resubmit the same `tid` through ValidateAndCommit inside an
/// ExternalReadEntry to assert that the row did not move before commit.
struct StatelessReadResult {
  bool found = false;   ///< True when the key existed and was non-empty.
  std::string value;    ///< Row payload, valid only when @ref found is true.
  uint64_t tid = 0;     ///< Packed (epoch | tid) version observed at read time.
};

/// One row from a primary-index range scan.
struct StatelessScanRow {
  std::string key;
  std::string value;
  uint64_t tid = 0;     ///< Packed version observed for this row.
  bool found = false;   ///< Tombstones are normally filtered out before this struct is produced.
};

/// One row from a secondary-index range scan.
///
/// `secondary_key` is the indexed key, `primary_key` is the base-table key
/// reached through it, and `value` is the corresponding base-table row.
struct StatelessSecondaryScanRow {
  std::string secondary_key;
  std::string primary_key;
  std::string value;
  uint64_t tid = 0;     ///< Packed version of the base row.
  bool found = false;
};

/// Token used to revalidate a range observed by a stateless scan.
///
/// ValidateAndCommit re-runs the scan described by `start_key`, `end_key`,
/// `row_limit`, and `reverse_scan`, and asserts that the observed key set
/// (`result_keys`, plus `result_primary_keys` on a secondary index) is
/// unchanged. Row TIDs are not part of this token; rows read from the range
/// are revalidated through their ExternalReadEntry records.
struct ExternalRangeValidationEntry {
  std::string table_name;
  std::string index_name;   ///< Empty marks a primary-index range.
  std::string start_key;    ///< Scan start (inclusive).
  std::string end_key;      ///< Scan end (exclusive). Must be non-empty.
  uint64_t row_limit = 0;   ///< Row cap applied during scan.
  bool reverse_scan = false;///< Scan direction.
  std::vector<std::string> result_keys;          ///< Observed key set.
  std::vector<std::string> result_primary_keys;  ///< Secondary index: paired primary keys.
};

/// Outcome of Database::StatelessRangeScan.
///
/// `ok` separates a genuine empty result from a Masstree retry that gave up
/// or a missing table. On `ok == false` the caller should abort the logical
/// transaction.
struct StatelessRangeScanResult {
  bool ok = false;
  std::vector<StatelessScanRow> rows;
  std::vector<ExternalRangeValidationEntry> range_versions;
};

/// Outcome of Database::StatelessSecondaryRangeScan.
struct StatelessSecondaryRangeScanResult {
  bool ok = false;
  std::vector<StatelessSecondaryScanRow> rows;
  std::vector<ExternalRangeValidationEntry> range_versions;
};

/// Caller-supplied record of a point read that ValidateAndCommit will
/// revalidate. Populate `tid` and `found` from an earlier StatelessRead.
struct ExternalReadEntry {
  std::string table_name;
  std::string key;
  uint64_t tid = 0;
  bool found = false;
};

/// Row write or delete to install during ValidateAndCommit.
///
/// When `is_delete` is true, `value` is ignored and the row is removed.
struct ExternalWriteEntry {
  std::string table_name;
  std::string key;
  std::string value;
  bool is_delete = false;
};

/// Secondary-index add or remove to install during ValidateAndCommit.
struct ExternalSecondaryIndexEntry {
  std::string table_name;
  std::string index_name;
  std::string secondary_key;
  std::string primary_key;
  bool is_delete = false;
};

}  // namespace LineairDB

#endif  // LINEAIRDB_STATELESS_H
