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

#include "thread_local_logger.h"

#include <fcntl.h>
#include <glob.h>
#include <lineairdb/database.h>
#include <lineairdb/tx_status.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <msgpack.hpp>
#include <util/logger.hpp>

#include "recovery/logger.h"
#include "types/definitions.h"

namespace LineairDB {
namespace Recovery {

std::atomic<size_t> ThreadLocalLogger::ThreadLocalStorageNode::ThreadIdCounter =
    {0};

ThreadLocalLogger::ThreadLocalLogger(const Config& config)
    : WorkingDir(config.work_dir),
      sync_log_writes_(std::getenv("LINEAIRDB_LOG_FSYNC") != nullptr &&
                       std::getenv("LINEAIRDB_LOG_FSYNC")[0] != '\0' &&
                       std::getenv("LINEAIRDB_LOG_FSYNC")[0] != '0') {
  LineairDB::Util::SetUpSPDLog();
}

void ThreadLocalLogger::RememberMe(const EpochNumber epoch) {
  auto* my_storage = thread_key_storage_.Get();
  my_storage->durable_epoch.store(epoch);
}

void ThreadLocalLogger::Enqueue(const WriteSetType& ws_ref, EpochNumber epoch,
                                bool entrusting) {
  if (ws_ref.empty()) return;

  /** Make log record and add it into local buffer  **/
  Recovery::Logger::LogRecord record;
  {
    record.epoch = epoch;

    for (auto& snapshot : ws_ref) {
      if (snapshot.index_name.empty()) {
        Logger::LogRecord::KeyValuePair kvp;
        kvp.key = snapshot.key;
        kvp.buffer = snapshot.data_item_copy.buffer.toString();
        kvp.tid = snapshot.data_item_copy.transaction_id.load();
        kvp.table_name = snapshot.table_name;
        kvp.index_name = snapshot.index_name;
        kvp.index_type = snapshot.index_type.Raw();
        kvp.primary_keys = snapshot.data_item_copy.primary_keys_vector();
        kvp.secondary_op = static_cast<uint8_t>(SecondaryIndexOp::None);
        record.key_value_pairs.emplace_back(std::move(kvp));
        continue;
      }

      if (snapshot.secondary_index_deltas.empty()) continue;
      for (const auto& delta : snapshot.secondary_index_deltas) {
        Logger::LogRecord::KeyValuePair kvp;
        kvp.key = snapshot.key;
        kvp.buffer = snapshot.data_item_copy.buffer.toString();
        kvp.tid = snapshot.data_item_copy.transaction_id.load();
        kvp.table_name = snapshot.table_name;
        kvp.index_name = snapshot.index_name;
        kvp.index_type = snapshot.index_type.Raw();
        kvp.secondary_op = static_cast<uint8_t>(delta.op);
        kvp.secondary_primary_key = delta.primary_key;
        record.key_value_pairs.emplace_back(std::move(kvp));
      }
    }
  }

  auto* my_storage = thread_key_storage_.Get();
  std::lock_guard<std::mutex> guard(my_storage->log_records_mutex);
  if (entrusting) {
    // A newly buffered record is not durable until a later epoch flush writes it.
    const EpochNumber durable_before_record = (epoch == 0) ? 0 : epoch - 1;
    const EpochNumber current_durable = my_storage->durable_epoch.load();
    if (current_durable == EpochFramework::THREAD_OFFLINE ||
        current_durable >= epoch) {
      my_storage->durable_epoch.store(durable_before_record);
    }
  }
  my_storage->log_records.emplace_back(std::move(record));
}

void ThreadLocalLogger::FlushLogs(EpochNumber stable_epoch) {
  FlushAllLogs(stable_epoch);
}

void ThreadLocalLogger::TruncateLogs(
    const EpochNumber checkpoint_completed_epoch) {
  auto* my_storage = thread_key_storage_.Get();
  std::lock_guard<std::mutex> guard(my_storage->log_records_mutex);

  assert(my_storage->truncated_epoch <= checkpoint_completed_epoch);
  if (checkpoint_completed_epoch == my_storage->truncated_epoch) return;
  auto log_filename = GetLogFileName(my_storage->thread_id);
  std::ifstream old_file(log_filename,
                         std::ifstream::in | std::ifstream::binary);

  std::string buffer((std::istreambuf_iterator<char>(old_file)),
                     std::istreambuf_iterator<char>());
  if (buffer.empty()) {
    my_storage->truncated_epoch = checkpoint_completed_epoch;

    return;
  }
  Logger::LogRecords records;
  Logger::LogRecords deserialized_records;
  size_t offset = 0;
  for (;;) {
    if (offset == buffer.size()) break;
    try {
      auto oh = msgpack::unpack(buffer.data(), buffer.size(), offset);
      auto obj = oh.get();
      obj.convert(deserialized_records);

    } catch (const std::bad_cast& e) {
      SPDLOG_ERROR(
          "  Stop recovery procedure: msgpack deserialize failure. Some "
          "records may not be recovered.");
      exit(EXIT_FAILURE);
    } catch (...) {
      SPDLOG_ERROR(
          "  Stop recovery procedure: msgpack deserialize failure. Some "
          "records may not be recovered.");
      exit(EXIT_FAILURE);
    }

    deserialized_records.erase(
        remove_if(deserialized_records.begin(), deserialized_records.end(),
                  [&](auto record) {
                    return record.epoch < checkpoint_completed_epoch;
                  }),
        deserialized_records.end());
    records.insert(records.end(), deserialized_records.begin(),
                   deserialized_records.end());
  }

  std::ofstream new_file(GetWorkingLogFileName(my_storage->thread_id));
  msgpack::pack(new_file, records);
  new_file.flush();

  // NOTE POSIX ensures that rename syscall provides atomicity
  const auto working_log_filename =
      GetWorkingLogFileName(my_storage->thread_id);
  if (rename(working_log_filename.c_str(),
             GetLogFileName(my_storage->thread_id).c_str())) {
    SPDLOG_ERROR("Durability Error: fail to truncate logfile. errno: {1}",
                 errno);
    exit(1);
  }
  my_storage->truncated_epoch = checkpoint_completed_epoch;
  my_storage->log_file = std::fstream(
      GetLogFileName(my_storage->thread_id),
      std::fstream::out | std::fstream::binary | std::fstream::ate);
}

EpochNumber ThreadLocalLogger::GetMinDurableEpochForAllThreads() {
  EpochNumber min_flushed_epoch = EpochFramework::THREAD_OFFLINE;
  thread_key_storage_.ForEach(
      [&](const ThreadLocalStorageNode* thread_local_node) {
        const EpochNumber epoch = thread_local_node->durable_epoch.load();
        if (epoch == EpochFramework::THREAD_OFFLINE) return;
        if (epoch < min_flushed_epoch) min_flushed_epoch = epoch;
      });
  return min_flushed_epoch;
}

std::string ThreadLocalLogger::GetLogFileName(size_t thread_id) const {
  // TODO: think of beautiful path concatation in C++
  return WorkingDir + "/thread" + std::to_string(thread_id) + ".log";
}

std::string ThreadLocalLogger::GetWorkingLogFileName(size_t thread_id) const {
  return WorkingDir + "/thread" + std::to_string(thread_id) + ".working.log";
}

void ThreadLocalLogger::FlushThreadLogs(ThreadLocalStorageNode* storage,
                                        EpochNumber stable_epoch) {
  std::lock_guard<std::mutex> guard(storage->log_records_mutex);
  if (!storage->log_records.empty()) {
    if (!storage->log_file.is_open()) {
      storage->log_file = std::fstream(
          GetLogFileName(storage->thread_id),
          std::fstream::out | std::fstream::binary | std::fstream::ate);
    }
    msgpack::pack(storage->log_file, storage->log_records);
    storage->log_file.flush();
    if (sync_log_writes_) {
      SyncLogFile(GetLogFileName(storage->thread_id));
    }
    storage->log_records.clear();
  }

  if (storage->durable_epoch.load() != EpochFramework::THREAD_OFFLINE) {
    storage->durable_epoch.store(stable_epoch);
  }
}

void ThreadLocalLogger::FlushAllLogs(EpochNumber stable_epoch) {
  std::lock_guard<std::mutex> guard(flush_all_mutex_);
  thread_key_storage_.ForEach([&](ThreadLocalStorageNode* storage) {
    FlushThreadLogs(storage, stable_epoch);
  });
}

void ThreadLocalLogger::SyncLogFile(const std::string& filename) const {
  const int fd = open(filename.c_str(), O_RDONLY);
  if (fd < 0) {
    SPDLOG_ERROR("Durability Error: fail to open logfile for fsync. errno: {0}",
                 errno);
    exit(1);
  }
  if (fsync(fd) != 0) {
    SPDLOG_ERROR("Durability Error: fail to fsync logfile. errno: {0}", errno);
    close(fd);
    exit(1);
  }
  close(fd);
}

}  // namespace Recovery
}  // namespace LineairDB
