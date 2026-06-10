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
                start_key, end_key, append_scan_entry)
          : table.value()->GetPrimaryIndex().Scan(
                start_key, end_key, append_scan_entry);
  if (!scan_result.has_value()) {
    result.rows.clear();
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
      return false;
    }

    auto slot = ConcurrencyControl::StableReadPrimaryKeys(*item);
    for (const auto& primary_key : slot.primary_keys) {
      if (snapshot_base_row(secondary_key, primary_key)) return true;
    }
    return false;
  };

  auto scan_result =
      reverse_scan
          ? index->ScanReverse(start_key, end_key, append_secondary_entry)
          : index->Scan(start_key, end_key, append_secondary_entry);
  if (!scan_result.has_value()) {
    result.rows.clear();
  }
  return result;
}

}  // namespace Stateless
}  // namespace LineairDB
