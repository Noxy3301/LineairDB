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
/// API does not: each call returns the snapshot (value, packed TID, observed
/// Masstree node versions) to the caller. The caller keeps those snapshots
/// across independent RPCs and resubmits them through Database::ValidateAndCommit
/// when the logical transaction is ready to commit. Because no state is
/// retained on the database side, calls can be spread across any thread.

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
/// One struct carries either of two forms:
///   - **Physical** (Masstree node version): `owner_ptr`, `node_ptr`, and
///     `version` are set, and `end_key` is empty. ValidateAndCommit replays
///     this through `ValidatePhantoms` to detect concurrent structural change
///     of the leaf.
///   - **Logical** (key list): `start_key`, `end_key`, `row_limit`,
///     `reverse_scan`, and `result_keys` (plus `result_primary_keys` on a
///     secondary index) are set. ValidateAndCommit re-runs the scan and
///     asserts the result set is unchanged.
///
/// The current implementation always emits the physical form. The logical
/// fields are reserved for a future switch.
struct ExternalRangeValidationEntry {
  std::string table_name;
  std::string index_name;   ///< Empty marks a primary-index range.
  uint64_t owner_ptr = 0;   ///< Physical: pointer to the IndexBase that owns the node.
  uint64_t node_ptr = 0;    ///< Physical: pointer to the Masstree node observed.
  uint64_t version = 0;     ///< Physical: node version captured at scan time.
  std::string start_key;    ///< Logical: scan start (inclusive).
  std::string end_key;      ///< Logical: scan end. Empty distinguishes the physical form.
  uint64_t row_limit = 0;   ///< Logical: row cap applied during scan.
  bool reverse_scan = false;///< Logical: scan direction.
  std::vector<std::string> result_keys;          ///< Logical: observed key set.
  std::vector<std::string> result_primary_keys;  ///< Logical (secondary): paired primary keys.
};

/// Token for a single exact index entry observed during a stateless scan.
///
/// Tombstones in the primary index and slots in a secondary index both fit
/// here. ValidateAndCommit rechecks the entry's TID after locking writes,
/// so a concurrent install over a tombstone or a rewrite of an SI slot is
/// turned into an abort at commit time.
struct ExternalIndexValidationEntry {
  std::string table_name;
  std::string index_name;   ///< Empty marks a primary-index entry such as a tombstone.
  std::string key;
  uint64_t tid = 0;         ///< Packed version observed at scan time.
  bool found = false;
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
  std::vector<ExternalIndexValidationEntry> index_reads;
};

/// Outcome of Database::StatelessSecondaryRangeScan.
struct StatelessSecondaryRangeScanResult {
  bool ok = false;
  std::vector<StatelessSecondaryScanRow> rows;
  std::vector<ExternalRangeValidationEntry> range_versions;
  std::vector<ExternalIndexValidationEntry> index_reads;
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
