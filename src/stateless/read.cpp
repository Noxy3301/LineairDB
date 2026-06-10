#include "stateless/read.h"

#include <mutex>

#include "concurrency_control/stable_read.hpp"
#include "index/secondary_index.h"
#include "stateless/packed_transaction_id.hpp"
#include "table/table.h"
#include "table/table_dictionary.hpp"
#include "types/data_item.hpp"

namespace LineairDB {
namespace Stateless {

StatelessReadResult Read(TableDictionary& tables,
                         std::shared_mutex& schema_mutex,
                         const std::string_view table_name,
                         const std::string_view key) {
  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  auto table = tables.GetTable(table_name);
  if (!table.has_value()) return {};

  DataItem* item = table.value()->GetPrimaryIndex().Get(key);
  if (item == nullptr) return {};

  auto row = ConcurrencyControl::StableReadValue(*item);
  return {row.found, std::move(row.value), PackTransactionId(row.tid)};
}

std::vector<StatelessReadResult> BatchRead(
    TableDictionary& tables, std::shared_mutex& schema_mutex,
    const std::vector<std::pair<std::string, std::string>>& keys) {
  std::vector<StatelessReadResult> results;
  results.reserve(keys.size());
  for (const auto& key : keys) {
    results.emplace_back(Read(tables, schema_mutex, key.first, key.second));
  }
  return results;
}

StatelessRangeScanResult RangeScan(TableDictionary& tables,
                                   std::shared_mutex& schema_mutex,
                                   const std::string_view table_name,
                                   const std::string_view start_key,
                                   const std::string_view end_key,
                                   uint64_t row_limit, bool reverse_scan) {
  StatelessRangeScanResult result;
  if (end_key.empty()) return result;

  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  auto table = tables.GetTable(table_name);
  if (!table.has_value()) return result;
  result.ok = true;

  // Commit re-walks the range and compares key lists, so we do not
  // ship masstree node pointers that RCU can free between RPCs.
  const bool use_logical_validation = true;
  std::vector<Index::NodeVersionEntry> versions;
  uint64_t returned_rows = 0;

  auto append_scan_entry = [&](std::string_view key, DataItem&) {
    DataItem* item = table.value()->GetPrimaryIndex().Get(key);
    if (item == nullptr) {
      return false;
    }

    auto row = ConcurrencyControl::StableReadValue(*item);
    if (row.found) {
      result.rows.push_back({std::string(key), std::move(row.value),
                             PackTransactionId(row.tid), true});
      ++returned_rows;
    }
    // Tombstones are skipped: Purge erases them at commit, and key-list
    // validation catches any reuse without needing a per-entry TID.
    return row_limit > 0 && returned_rows >= row_limit;
  };

  auto scan_result =
      reverse_scan
          ? table.value()->GetPrimaryIndex().ScanReverse(
                start_key, end_key, append_scan_entry,
                use_logical_validation ? nullptr : &versions)
          : table.value()->GetPrimaryIndex().Scan(
                start_key, end_key, append_scan_entry,
                use_logical_validation ? nullptr : &versions);
  if (!scan_result.has_value()) {
    result.rows.clear();
    result.range_versions.clear();
    result.index_reads.clear();
    return result;
  }

  if (use_logical_validation) {
    ExternalRangeValidationEntry logical_range;
    logical_range.table_name = std::string(table_name);
    logical_range.start_key = std::string(start_key);
    logical_range.end_key = std::string(end_key);
    logical_range.row_limit = row_limit;
    logical_range.reverse_scan = reverse_scan;
    logical_range.result_keys.reserve(result.rows.size());
    for (const auto& row : result.rows) {
      logical_range.result_keys.push_back(row.key);
    }
    result.range_versions.push_back(std::move(logical_range));
  } else {
    result.range_versions.reserve(versions.size());
    for (const auto& version : versions) {
      ExternalRangeValidationEntry entry;
      entry.table_name = std::string(table_name);
      entry.owner_ptr = reinterpret_cast<uint64_t>(version.owner);
      entry.node_ptr = reinterpret_cast<uint64_t>(version.node_ptr);
      entry.version = version.version;
      result.range_versions.push_back(std::move(entry));
    }
  }
  return result;
}

StatelessSecondaryRangeScanResult SecondaryRangeScan(
    TableDictionary& tables, std::shared_mutex& schema_mutex,
    const std::string_view table_name, const std::string_view index_name,
    const std::string_view start_key, const std::string_view end_key,
    uint64_t row_limit, bool reverse_scan) {
  StatelessSecondaryRangeScanResult result;
  if (end_key.empty()) return result;

  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  auto table = tables.GetTable(table_name);
  if (!table.has_value()) return result;

  Index::SecondaryIndex* index = table.value()->GetSecondaryIndex(index_name);
  if (index == nullptr) return result;
  result.ok = true;

  // Same rationale as RangeScan above.
  const bool use_logical_validation = true;
  std::vector<Index::NodeVersionEntry> versions;
  uint64_t returned_rows = 0;

  auto snapshot_base_row = [&](const std::string& secondary_key,
                               const std::string& primary_key) {
    DataItem* item = table.value()->GetPrimaryIndex().Get(primary_key);
    if (item == nullptr) {
      return false;
    }

    auto row = ConcurrencyControl::StableReadValue(*item);
    if (row.found) {
      result.rows.push_back({secondary_key, primary_key, std::move(row.value),
                             PackTransactionId(row.tid), true});
      ++returned_rows;
    }
    return row_limit > 0 && returned_rows >= row_limit;
  };

  auto append_secondary_entry = [&](std::string_view key) {
    const std::string secondary_key(key);
    DataItem* item = index->Get(key);
    if (item == nullptr) {
      if (!use_logical_validation) {
        result.index_reads.push_back({std::string(table_name),
                                      std::string(index_name), secondary_key,
                                      0, false});
      }
      return false;
    }

    auto slot = ConcurrencyControl::StableReadPrimaryKeys(*item);
    if (!use_logical_validation) {
      result.index_reads.push_back(
          {std::string(table_name), std::string(index_name), secondary_key,
           PackTransactionId(slot.tid), slot.found});
    }

    for (const auto& primary_key : slot.primary_keys) {
      if (snapshot_base_row(secondary_key, primary_key)) return true;
    }
    return false;
  };

  auto scan_result =
      reverse_scan
          ? index->ScanReverse(start_key, end_key, append_secondary_entry,
                               use_logical_validation ? nullptr : &versions)
          : index->Scan(start_key, end_key, append_secondary_entry,
                        use_logical_validation ? nullptr : &versions);
  if (!scan_result.has_value()) {
    result.rows.clear();
    result.range_versions.clear();
    result.index_reads.clear();
    return result;
  }

  if (use_logical_validation) {
    ExternalRangeValidationEntry logical_range;
    logical_range.table_name = std::string(table_name);
    logical_range.index_name = std::string(index_name);
    logical_range.start_key = std::string(start_key);
    logical_range.end_key = std::string(end_key);
    logical_range.row_limit = row_limit;
    logical_range.reverse_scan = reverse_scan;
    logical_range.result_keys.reserve(result.rows.size());
    logical_range.result_primary_keys.reserve(result.rows.size());
    for (const auto& row : result.rows) {
      logical_range.result_keys.push_back(row.secondary_key);
      logical_range.result_primary_keys.push_back(row.primary_key);
    }
    result.range_versions.push_back(std::move(logical_range));
  } else {
    result.range_versions.reserve(versions.size());
    for (const auto& version : versions) {
      ExternalRangeValidationEntry entry;
      entry.table_name = std::string(table_name);
      entry.index_name = std::string(index_name);
      entry.owner_ptr = reinterpret_cast<uint64_t>(version.owner);
      entry.node_ptr = reinterpret_cast<uint64_t>(version.node_ptr);
      entry.version = version.version;
      result.range_versions.push_back(std::move(entry));
    }
  }
  return result;
}

}  // namespace Stateless
}  // namespace LineairDB
