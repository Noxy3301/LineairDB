/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/tx_status.h>

#include <functional>
#include <memory>

#include "database_impl.h"
#include "util/logger.hpp"
namespace LineairDB {

Database::Database() noexcept : db_pimpl_(std::make_unique<Impl>()) {
  LineairDB::Util::SetUpSPDLog();
}
Database::Database(const Config& c) noexcept
    : db_pimpl_(std::make_unique<Impl>(c)) {
  LineairDB::Util::SetUpSPDLog();
}

Database::~Database() noexcept = default;

const Config Database::GetConfig() const noexcept {
  return db_pimpl_->GetConfig();
}

void Database::ExecuteTransaction(
    std::function<void(Transaction&)> transaction_procedure,
    std::function<void(TxStatus)> callback,
    std::optional<CallbackType> precommit_clbk) {
  db_pimpl_->ExecuteTransaction(transaction_procedure, callback,
                                precommit_clbk);
}

Transaction& Database::BeginTransaction() {
  return db_pimpl_->BeginTransaction();
}

bool Database::EndTransaction(Transaction& tx, CallbackType clbk) {
  return db_pimpl_->EndTransaction(std::forward<decltype(tx)>(tx),
                                   std::forward<decltype(clbk)>(clbk));
}

void Database::Fence() const noexcept { db_pimpl_->Fence(); }
void Database::WaitForCheckpoint() const noexcept {
  db_pimpl_->WaitForCheckpoint();
}
void Database::RequestCallbacks() { db_pimpl_->RequestCallbacks(); }
void Database::ReleaseMasstreeThreadEpoch() {
  Index::MasstreeReleaseThreadEpoch();
}
void Database::FullyDrainMasstreeThread() {
  Index::MasstreeFullyDrainThread();
}
bool Database::CreateTable(const std::string_view table_name) {
  return db_pimpl_->CreateTable(table_name);
}

bool Database::InstallPaxSchema(const std::string_view table_name,
                                const std::vector<uint32_t>& field_max_bytes) {
  return db_pimpl_->InstallPaxSchema(table_name, field_max_bytes);
}

Pax::PaxStore* Database::GetPaxStore(const std::string_view table_name) {
  return db_pimpl_->GetPaxStore(table_name);
}

bool Database::CreateSecondaryIndex(const std::string_view table_name,
                                    const std::string_view index_name,
                                    const uint index_type) {
  return db_pimpl_->CreateSecondaryIndex(table_name, index_name, index_type);
}

StatelessReadResult Database::StatelessRead(
    const std::string_view table_name, const std::string_view key,
    const std::vector<uint32_t>* selected_columns) {
  return db_pimpl_->StatelessRead(table_name, key, selected_columns);
}

std::vector<StatelessReadResult> Database::StatelessBatchRead(
    const std::vector<std::pair<std::string, std::string>>& keys) {
  return db_pimpl_->StatelessBatchRead(keys);
}

StatelessRangeScanResult Database::StatelessRangeScan(
    const std::string_view table_name, const std::string_view start_key,
    const std::string_view end_key, uint64_t row_limit, bool reverse_scan,
    const std::vector<uint32_t>* selected_columns) {
  return db_pimpl_->StatelessRangeScan(table_name, start_key, end_key,
                                       row_limit, reverse_scan,
                                       selected_columns);
}

StatelessPaxRowRefScanResult Database::StatelessPaxRowRefScan(
    const std::string_view table_name, const std::string_view start_key,
    const std::string_view end_key, uint64_t row_limit, bool reverse_scan) {
  return db_pimpl_->StatelessPaxRowRefScan(table_name, start_key, end_key,
                                        row_limit, reverse_scan);
}

StatelessSecondaryRangeScanResult Database::StatelessSecondaryRangeScan(
    const std::string_view table_name, const std::string_view index_name,
    const std::string_view start_key, const std::string_view end_key,
    uint64_t row_limit, bool reverse_scan,
    const std::vector<uint32_t>* selected_columns) {
  return db_pimpl_->StatelessSecondaryRangeScan(table_name, index_name,
                                                start_key, end_key, row_limit,
                                                reverse_scan,
                                                selected_columns);
}

bool Database::ComputeIndexNdvInt(const std::string_view table_name,
                                  const std::string_view index_name,
                                  uint32_t num_parts,
                                  std::vector<uint64_t>& out_ndv) {
  return db_pimpl_->ComputeIndexNdvInt(table_name, index_name, num_parts,
                                       out_ndv);
}

bool Database::ComputeIndexHistogram(const std::string_view table_name,
                                     const std::string_view index_name,
                                     uint32_t buckets,
                                     std::vector<std::string>& out_bounds,
                                     std::vector<uint64_t>& out_cum) {
  return db_pimpl_->ComputeIndexHistogram(table_name, index_name, buckets,
                                          out_bounds, out_cum);
}

bool Database::ValidateAndCommit(
    const std::vector<ExternalReadEntry>& reads,
    const std::vector<ExternalWriteEntry>& writes,
    const std::vector<ExternalSecondaryIndexEntry>& secondary_index_ops,
    const std::vector<ExternalRangeReadEntry>& range_reads,
    std::string* abort_reason) {
  return db_pimpl_->ValidateAndCommit(reads, writes, secondary_index_ops,
                                      range_reads, abort_reason);
}

}  // namespace LineairDB
