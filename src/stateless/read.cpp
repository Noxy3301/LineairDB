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
                         const std::string_view key,
                         const std::vector<uint32_t>* sparse_columns) {
  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  auto table = tables.GetTable(table_name);
  if (!table.has_value()) return {};

  DataItem* item = table.value()->GetPrimaryIndex().Get(key);
  if (item == nullptr) return {};

  auto row = sparse_columns != nullptr
                 ? ConcurrencyControl::StableReadValueSparse(
                       *item, sparse_columns->data(), sparse_columns->size())
                 : ConcurrencyControl::StableReadValue(*item);
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
                                   uint64_t row_limit, bool reverse_scan,
                                   const std::vector<uint32_t>* sparse_columns) {
  StatelessRangeScanResult result;
  if (end_key.empty()) return result;

  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  auto table = tables.GetTable(table_name);
  if (!table.has_value()) return result;
  result.ok = true;

  uint64_t returned_rows = 0;

  // The value-yielding Scan/ScanReverse overloads pass the DataItem the leaf
  // walk already resolved, so read it directly instead of re-fetching by key.
  auto append_scan_entry = [&](std::string_view key, DataItem& item_ref) {
    auto row = sparse_columns != nullptr
                   ? ConcurrencyControl::StableReadValueSparse(
                         item_ref, sparse_columns->data(),
                         sparse_columns->size())
                   : ConcurrencyControl::StableReadValue(item_ref);
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
    result.ok = false;
    result.rows.clear();
  }
  return result;
}

}  // namespace Stateless
uint64_t PaxRefCurrentTid(const StatelessPaxRefRow& row) {
  const auto* item = static_cast<const DataItem*>(row.item);
  return Stateless::PackTransactionId(item->transaction_id.load());
}
namespace Stateless {

StatelessPaxRefScanResult PaxRefScan(TableDictionary& tables,
                                     std::shared_mutex& schema_mutex,
                                     const std::string_view table_name,
                                     const std::string_view start_key,
                                     const std::string_view end_key,
                                     uint64_t row_limit, bool reverse_scan) {
  StatelessPaxRefScanResult result;
  if (end_key.empty()) return result;

  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  auto table = tables.GetTable(table_name);
  if (!table.has_value()) return result;
  auto* store = table.value()->GetPaxStore();
  // Heap-fallback rows are invisible to strips; refuse the whole scan.
  if (store == nullptr || store->overflow_count() > 0) return result;
  result.ok = true;

  uint64_t returned_rows = 0;
  bool saw_non_pax = false;

  auto append_ref = [&](std::string_view key, DataItem& item_ref) {
    // Stable TID observation point (same spin-on-lock as StableReadValue);
    // the caller re-checks this TID after reading the row's cells.
    TransactionId tid;
    for (;;) {
      tid = item_ref.transaction_id.load();
      if (!(tid.tid & 1u)) break;
      _mm_pause();
    }
    const size_t sz = item_ref.buffer.size;
    if (sz == 0) return false;  // tombstone: skip, keep scanning
    if (!item_ref.buffer.is_pax() || !item_ref.buffer.pax_allocated()) {
      saw_non_pax = true;
      return true;  // cancel: mixed storage, caller must materialize
    }
    result.rows.push_back({std::string(key), item_ref.buffer.pax_group(),
                           item_ref.buffer.pax_slot(),
                           static_cast<uint32_t>(sz), PackTransactionId(tid),
                           &item_ref});
    ++returned_rows;
    return row_limit > 0 && returned_rows >= row_limit;
  };

  auto scan_result =
      reverse_scan ? table.value()->GetPrimaryIndex().ScanReverse(
                         start_key, end_key, append_ref)
                   : table.value()->GetPrimaryIndex().Scan(start_key, end_key,
                                                           append_ref);
  if (!scan_result.has_value() || saw_non_pax) {
    result.ok = false;
    result.rows.clear();
  }
  return result;
}

StatelessSecondaryRangeScanResult SecondaryRangeScan(
    TableDictionary& tables, std::shared_mutex& schema_mutex,
    const std::string_view table_name, const std::string_view index_name,
    const std::string_view start_key, const std::string_view end_key,
    uint64_t row_limit, bool reverse_scan,
    const std::vector<uint32_t>* sparse_columns) {
  StatelessSecondaryRangeScanResult result;
  if (end_key.empty()) return result;

  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  auto table = tables.GetTable(table_name);
  if (!table.has_value()) return result;

  Index::SecondaryIndex* index = table.value()->GetSecondaryIndex(index_name);
  if (index == nullptr) return result;
  result.ok = true;

  uint64_t returned_rows = 0;

  auto append_base_row = [&](std::string_view secondary_key,
                             std::string_view primary_key) {
    DataItem* item = table.value()->GetPrimaryIndex().Get(primary_key);
    if (item == nullptr) {
      return false;
    }

    auto row = sparse_columns != nullptr
                   ? ConcurrencyControl::StableReadValueSparse(
                         *item, sparse_columns->data(), sparse_columns->size())
                   : ConcurrencyControl::StableReadValue(*item);
    if (row.found) {
      result.rows.push_back({std::string(secondary_key),
                             std::string(primary_key), std::move(row.value),
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
    for (std::string_view primary_key : slot.primary_keys_view()) {
      if (append_base_row(secondary_key, primary_key)) return true;
    }
    return false;
  };

  auto scan_result =
      reverse_scan
          ? index->ScanReverse(start_key, end_key, append_secondary_entry)
          : index->Scan(start_key, end_key, append_secondary_entry);
  if (!scan_result.has_value()) {
    result.ok = false;
    result.rows.clear();
  }
  return result;
}

}  // namespace Stateless
}  // namespace LineairDB
