#include "epoch_scan_checkpoint.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <xmmintrin.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <msgpack.hpp>
#include <string>
#include <utility>
#include <vector>

#include "crc32c.h"
#include "index/concurrent_table.h"
#include "index/impl/masstree_index.hpp"
#include "index/secondary_index.h"
#include "logger.h"
#include "table/table.h"
#include "table/table_dictionary.hpp"
#include "types/data_item.hpp"
#include "util/debug_sync.hpp"
#include "util/epoch_framework.hpp"
#include "util/logger.hpp"

namespace LineairDB {
namespace Recovery {

namespace {

using Clock = std::chrono::steady_clock;

// How long one row is spun on before it is set aside for the retry pass. A
// writer holds a row for the length of its install, so a short spin covers the
// common case and a longer wait belongs to the pass that runs without holding
// up the rest of the table.
constexpr unsigned kSpinAttempts = 64;
// The retry pass gives up eventually rather than scanning forever, because a
// row that never settles means the image cannot be written at all.
constexpr unsigned kRetryRounds = 200;
constexpr auto kRetryPause = std::chrono::milliseconds(25);
// The frontier advances once per epoch, so a wait beyond this means the
// flusher is not running rather than that the epoch is slow.
constexpr auto kDurabilityWait = std::chrono::seconds(60);

void PutLe16(uint8_t* out, uint16_t value) {
  out[0] = static_cast<uint8_t>(value & 0xffu);
  out[1] = static_cast<uint8_t>((value >> 8) & 0xffu);
}

void PutLe32(uint8_t* out, uint32_t value) {
  for (size_t i = 0; i < 4; ++i) {
    out[i] = static_cast<uint8_t>((value >> (8 * i)) & 0xffu);
  }
}

void PutLe64(uint8_t* out, uint64_t value) {
  for (size_t i = 0; i < 8; ++i) {
    out[i] = static_cast<uint8_t>((value >> (8 * i)) & 0xffu);
  }
}

uint16_t GetLe16(const uint8_t* in) {
  return static_cast<uint16_t>(static_cast<uint16_t>(in[0]) |
                               static_cast<uint16_t>(in[1] << 8));
}

uint32_t GetLe32(const uint8_t* in) {
  uint32_t value = 0;
  for (size_t i = 0; i < 4; ++i) {
    value |= static_cast<uint32_t>(in[i]) << (8 * i);
  }
  return value;
}

uint64_t GetLe64(const uint8_t* in) {
  uint64_t value = 0;
  for (size_t i = 0; i < 8; ++i) {
    value |= static_cast<uint64_t>(in[i]) << (8 * i);
  }
  return value;
}

int64_t ElapsedMs(Clock::time_point from) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() -
                                                               from)
      .count();
}

bool WriteAll(int fd, const void* data, size_t size) {
  const auto* bytes = static_cast<const uint8_t*>(data);
  while (size != 0) {
    const ssize_t written = ::write(fd, bytes, size);
    if (written > 0) {
      bytes += written;
      size -= static_cast<size_t>(written);
      continue;
    }
    if (written < 0 && errno == EINTR) continue;
    return false;
  }
  return true;
}

bool ReadAll(int fd, void* data, size_t size, off_t offset) {
  auto* bytes = static_cast<uint8_t*>(data);
  while (size != 0) {
    const ssize_t got = ::pread(fd, bytes, size, offset);
    if (got > 0) {
      bytes += got;
      offset += got;
      size -= static_cast<size_t>(got);
      continue;
    }
    if (got < 0 && errno == EINTR) continue;
    return false;
  }
  return true;
}

// Closes a descriptor on every path out of a function, including one an
// allocation left through.
struct OpenFile {
  explicit OpenFile(int descriptor) : fd(descriptor) {}
  ~OpenFile() {
    if (fd >= 0) ::close(fd);
  }
  OpenFile(const OpenFile&) = delete;
  OpenFile& operator=(const OpenFile&) = delete;
  int fd;
};

int FsyncRetryingOnInterrupt(int fd) {
  int rc;
  do {
    rc = ::fsync(fd);
  } while (rc < 0 && errno == EINTR);
  return rc;
}

// A file's own fsync does not make its name durable, and the name is what the
// rename publishes.
bool FsyncDirectory(const std::string& directory) {
  const int fd =
      ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) return false;
  const bool ok = FsyncRetryingOnInterrupt(fd) == 0;
  ::close(fd);
  return ok;
}

}  // namespace

const char* EpochScanCheckpoint::ImageFileName() { return "checkpoint.img"; }
const char* EpochScanCheckpoint::WorkingFileName() {
  return "checkpoint.working";
}

/**
 * Copies one row's bytes together with the version they belong to.
 *
 * The protocol is the read path's: refuse a locked version, copy, then confirm
 * the version did not move. A row that stays locked for the whole budget is
 * reported as unstable rather than skipped, because the transaction holding it
 * may still abort and leave no record of the value the image would be missing.
 */
EpochScanCheckpoint::Capture EpochScanCheckpoint::CapturePrimaryRow(
    const std::string& table_name, std::string_view key, const DataItem& item,
    LogRecord::KeyValuePair* out, uint64_t* retries) {
  // FIXME: the copy reads the row's storage without pinning it, as the read
  // path does, so an install that reallocates it during the copy is caught by
  // the version recheck rather than prevented
  for (unsigned attempt = 0; attempt < kSpinAttempts; ++attempt) {
    const TransactionId first = item.transaction_id.load();
    if (first.tid & 1u) {
      ++*retries;
      _mm_pause();
      continue;
    }
    LINEAIRDB_DEBUG_SYNC("checkpoint.before_row_copy");
    if (!item.IsPrimaryInitialized()) {
      // A blank slot or a tombstone, once the version confirms the emptiness
      // is not the middle of an install.
      if (item.transaction_id.load() == first) {
        return EpochScanCheckpoint::Capture::Skipped;
      }
      ++*retries;
      continue;
    }
    std::string bytes = item.buffer.toString();
    if (item.transaction_id.load() != first) {
      ++*retries;
      continue;
    }
    out->key.assign(key.data(), key.size());
    out->buffer = std::move(bytes);
    out->tid = first;
    out->table_name = table_name;
    out->secondary_op = static_cast<uint8_t>(SecondaryIndexOp::None);
    return EpochScanCheckpoint::Capture::Taken;
  }
  return EpochScanCheckpoint::Capture::Unstable;
}

/**
 * Copies one secondary key's whole primary-key list under the same protocol.
 *
 * The list is written as a complete posting list rather than as a delta, so a
 * later delta in the log composes with it the way one delta composes with
 * another.
 */
EpochScanCheckpoint::Capture EpochScanCheckpoint::CaptureSecondaryEntry(
    const std::string& table_name, const std::string& index_name,
    uint32_t index_type, std::string_view key, const DataItem& item,
    LogRecord::KeyValuePair* out, uint64_t* retries) {
  for (unsigned attempt = 0; attempt < kSpinAttempts; ++attempt) {
    const TransactionId first = item.transaction_id.load();
    if (first.tid & 1u) {
      ++*retries;
      _mm_pause();
      continue;
    }
    auto primary_keys = std::atomic_load(&item.primary_keys_);
    if (item.transaction_id.load() != first) {
      ++*retries;
      continue;
    }
    const PackedPrimaryKeysView keys(primary_keys);
    if (keys.empty()) return EpochScanCheckpoint::Capture::Skipped;
    out->key.assign(key.data(), key.size());
    out->tid = first;
    out->table_name = table_name;
    out->index_name = index_name;
    out->index_type = index_type;
    out->primary_keys.reserve(keys.size());
    for (std::string_view primary_key : keys) {
      out->primary_keys.emplace_back(primary_key.data(), primary_key.size());
    }
    out->secondary_op = static_cast<uint8_t>(SecondaryIndexOp::Full);
    return EpochScanCheckpoint::Capture::Taken;
  }
  return EpochScanCheckpoint::Capture::Unstable;
}

EpochScanCheckpoint::EpochScanCheckpoint(const Config& config,
                                         TableDictionary& tables,
                                         EpochFramework& epoch_framework,
                                         Logger& logger)
    : config_(config),
      tables_(tables),
      epoch_framework_(epoch_framework),
      logger_(logger),
      image_path_((std::filesystem::path(config.work_dir) / ImageFileName())
                      .string()),
      working_path_(
          (std::filesystem::path(config.work_dir) / WorkingFileName())
              .string()) {}

EpochScanCheckpoint::~EpochScanCheckpoint() { Stop(); }

bool EpochScanCheckpoint::Supported() const {
  if (!config_.enable_logging) {
    SPDLOG_WARN(
        "No checkpoint image is written: an image is merged with the log at "
        "recovery, and this durability contract writes no log");
    return false;
  }
  if (config_.index_structure != Config::IndexStructure::Masstree) {
    SPDLOG_WARN(
        "No checkpoint image is written: the scan walks the index with the "
        "Masstree backend's iteration contract");
    return false;
  }
  return true;
}

void EpochScanCheckpoint::Start() {
  if (config_.checkpoint_interval_ms == 0 &&
      config_.checkpoint_once_after_ms == 0) {
    return;
  }
  if (!Supported()) return;
  thread_ = std::thread([this]() { Loop(); });
}

void EpochScanCheckpoint::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_ = true;
  }
  cv_.notify_all();
  if (thread_.joinable()) thread_.join();
}

bool EpochScanCheckpoint::WaitFor(uint64_t milliseconds) {
  std::unique_lock<std::mutex> lock(mutex_);
  cv_.wait_for(lock, std::chrono::milliseconds(milliseconds),
               [this] { return stop_; });
  return !stop_;
}

void EpochScanCheckpoint::Loop() {
  if (config_.checkpoint_once_after_ms != 0) {
    if (!WaitFor(config_.checkpoint_once_after_ms)) return;
    RunOnce();
    if (config_.checkpoint_interval_ms == 0) return;
  }
  while (WaitFor(config_.checkpoint_interval_ms)) {
    RunOnce();
  }
}

bool EpochScanCheckpoint::RunOnce(Stats* out_stats) {
  if (!Supported()) return false;

  std::unique_lock<std::mutex> capture(capture_mutex_, std::try_to_lock);
  if (!capture.owns_lock()) {
    SPDLOG_WARN("A checkpoint image is already being written");
    return false;
  }

  Stats stats;
  stats.generation = ++generation_;

  const auto barrier_begin = Clock::now();
  // The cut is read before the barrier and the scan begins after it: once Sync
  // returns, no thread is online in an epoch at or below the cut, so every
  // commit that belongs to the replay's lower bound has already installed its
  // values. Taking the scan's start time as the bound instead would drop the
  // commits still in flight when it was taken.
  stats.cut_epoch = epoch_framework_.GetGlobalEpoch();
  // Read here, ahead of the barrier and the scan, because both give the log's
  // frontier a chance to tick past what any frame on disk actually holds: the
  // epoch clock keeps advancing on its own timer, closed epochs are handed to
  // the flusher whether or not anything landed in them, and a closed empty
  // epoch moves the durable epoch forward without writing a frame. Recovery's
  // acceptance gate needs a bound a quiet log can still meet, so this is taken
  // as close to "whatever the log last actually held" as the code has a name
  // for.
  stats.wal_frontier_at_publish = logger_.GetDurableEpoch();
  epoch_framework_.Sync();
  stats.barrier_ms = ElapsedMs(barrier_begin);

  const auto scan_begin = Clock::now();
  LogRecords records;
  bool abandoned = false;
  tables_.ForEachTable([&](Table& table) {
    if (abandoned) return;
    LogRecord record;
    record.epoch = stats.cut_epoch;
    if (!CaptureTable(table, &record, &stats)) {
      abandoned = true;
      return;
    }
    if (!record.key_value_pairs.empty()) {
      records.emplace_back(std::move(record));
    }
  });

  // Ends the index's reclamation critical section, which the pass held open
  // from its first walk so that a row retired during it could not be freed
  // under the copy. Everything the pass keeps has been copied out by now, and
  // an index reclaims nothing at all while a thread is inside one.
  Index::MasstreeReleaseThreadEpoch();

  stats.scan_ms = ElapsedMs(scan_begin);
  // Every version in the image was published at or below this epoch, which is
  // what the durability gate below is asked about.
  stats.end_epoch = epoch_framework_.GetGlobalEpoch();

  if (abandoned) {
    ::unlink(working_path_.c_str());
    SPDLOG_WARN(
        "Checkpoint {0} abandoned: a row did not present a stable version",
        stats.generation);
    if (out_stats != nullptr) *out_stats = stats;
    return false;
  }

  const bool published = Publish(records, &stats);
  if (out_stats != nullptr) *out_stats = stats;
  if (!published) return false;

  SPDLOG_INFO(
      "Checkpoint {0} written: {1} rows, {2} index entries, {3} bytes, cut "
      "epoch {4}, end epoch {5}, {6} ms at the barrier, {7} ms scanning, {8} "
      "ms writing, {9} ms waiting for the log, {10} version retries",
      stats.generation, stats.primary_rows, stats.secondary_entries,
      stats.image_bytes, stats.cut_epoch, stats.end_epoch, stats.barrier_ms,
      stats.scan_ms, stats.write_ms, stats.gate_ms, stats.retries);
  return true;
}

bool EpochScanCheckpoint::CaptureTable(Table& table, LogRecord* record,
                                       Stats* stats) {
  const std::string& table_name = table.GetTableName();
  std::vector<std::string> unstable_rows;
  std::vector<std::pair<std::string, std::string>> unstable_entries;

  // The walk's callback returns whether to stop, which is the Masstree
  // backend's reading of it and the opposite of the hash backend's; startup
  // refuses any other index structure rather than write a one-row image.
  table.GetPrimaryIndex().ForEach([&](std::string_view key, DataItem& item) {
    LogRecord::KeyValuePair kvp;
    switch (CapturePrimaryRow(table_name, key, item, &kvp, &stats->retries)) {
      case Capture::Taken:
        ++stats->primary_rows;
        record->key_value_pairs.emplace_back(std::move(kvp));
        break;
      case Capture::Skipped:
        break;
      case Capture::Unstable:
        unstable_rows.emplace_back(key.data(), key.size());
        break;
    }
    return false;
  });

  table.ForEachSecondaryIndex([&](const std::string& index_name,
                                  Index::SecondaryIndex& index) {
    const uint32_t index_type = index.GetIndexType().Raw();
    index.ForEach([&](std::string_view key, DataItem& item) {
      LogRecord::KeyValuePair kvp;
      switch (CaptureSecondaryEntry(table_name, index_name, index_type, key,
                                    item, &kvp, &stats->retries)) {
        case Capture::Taken:
          ++stats->secondary_entries;
          record->key_value_pairs.emplace_back(std::move(kvp));
          break;
        case Capture::Skipped:
          break;
        case Capture::Unstable:
          unstable_entries.emplace_back(index_name,
                                        std::string(key.data(), key.size()));
          break;
      }
      return false;
    });
  });

  // Rows held by a writer for the whole spin are resolved again by key: the
  // slot they were in may have been purged and replaced meanwhile, and a
  // pointer kept across the pass would name the old one.
  bool stopped = false;
  for (unsigned round = 0; round < kRetryRounds; ++round) {
    if (unstable_rows.empty() && unstable_entries.empty()) break;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopped = stop_;
    }
    if (stopped) break;
    std::this_thread::sleep_for(kRetryPause);

    std::vector<std::string> rows_left;
    for (const auto& key : unstable_rows) {
      DataItem* item = table.GetPrimaryIndex().Get(key);
      if (item == nullptr) continue;
      LogRecord::KeyValuePair kvp;
      switch (CapturePrimaryRow(table_name, key, *item, &kvp,
                                &stats->retries)) {
        case Capture::Taken:
          ++stats->primary_rows;
          record->key_value_pairs.emplace_back(std::move(kvp));
          break;
        case Capture::Skipped:
          break;
        case Capture::Unstable:
          rows_left.emplace_back(key);
          break;
      }
    }
    unstable_rows.swap(rows_left);

    std::vector<std::pair<std::string, std::string>> entries_left;
    for (const auto& [index_name, key] : unstable_entries) {
      Index::SecondaryIndex* index = table.GetSecondaryIndex(index_name);
      if (index == nullptr) continue;
      DataItem* item = index->Get(key);
      if (item == nullptr) continue;
      LogRecord::KeyValuePair kvp;
      switch (CaptureSecondaryEntry(table_name, index_name,
                                    index->GetIndexType().Raw(), key, *item,
                                    &kvp, &stats->retries)) {
        case Capture::Taken:
          ++stats->secondary_entries;
          record->key_value_pairs.emplace_back(std::move(kvp));
          break;
        case Capture::Skipped:
          break;
        case Capture::Unstable:
          entries_left.emplace_back(index_name, key);
          break;
      }
    }
    unstable_entries.swap(entries_left);
  }

  return !stopped && unstable_rows.empty() && unstable_entries.empty();
}

bool EpochScanCheckpoint::Publish(const LogRecords& records, Stats* stats) {
  msgpack::sbuffer payload;
  msgpack::pack(payload, records);

  // The image holds versions the scan saw during it, and a version whose
  // record never reached the device would come back alone, without the rest of
  // the transaction that wrote it. Waiting until the log covers the last
  // epoch the scan could have observed rules that out, and it is done before
  // the header below is built because wal_frontier_at_publish is read from
  // the same wait: every record this gate is waiting for is, by the time it
  // returns, a frame already on disk, which is what lets recovery trust that
  // bound as a floor on what the log still holds.
  const auto gate_begin = Clock::now();
  if (config_.enable_logging) {
    const auto result =
        logger_.WaitUntilDurable(stats->end_epoch, Clock::now() +
                                                       kDurabilityWait);
    if (result != Logger::WaitResult::Durable) {
      // Nothing has been written to working_path_ this round, but a file
      // left behind by an earlier, differently-failed attempt still might be
      // there; a discarded checkpoint should not let one linger.
      ::unlink(working_path_.c_str());
      SPDLOG_WARN(
          "Checkpoint {0} discarded: the log did not become durable through "
          "epoch {1}",
          stats->generation, stats->end_epoch);
      return false;
    }
    stats->wal_frontier_at_publish = logger_.GetWalFrontier();
  }
  stats->gate_ms = ElapsedMs(gate_begin);

  const auto write_begin = Clock::now();
  uint8_t header[kHeaderSize];
  std::memset(header, 0, sizeof(header));
  PutLe32(header, kMagic);
  PutLe16(header + 4, kVersion);
  PutLe16(header + 6, kFlags);
  PutLe64(header + 8, stats->generation);
  PutLe32(header + 16, stats->cut_epoch);
  PutLe32(header + 20, stats->end_epoch);
  PutLe32(header + 24, stats->wal_frontier_at_publish);
  PutLe64(header + 28, stats->primary_rows);
  PutLe64(header + 36, stats->secondary_entries);
  PutLe64(header + 44, static_cast<uint64_t>(payload.size()));
  Crc32c crc;
  crc.Update(header, kHeaderSize - sizeof(uint32_t));
  crc.Update(payload.data(), payload.size());
  PutLe32(header + kHeaderSize - sizeof(uint32_t), crc.Finish());
  stats->image_bytes = kHeaderSize + payload.size();

  const int fd = ::open(working_path_.c_str(),
                        O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    SPDLOG_WARN("Checkpoint {0} could not open {1} (errno {2})",
                stats->generation, working_path_, errno);
    return false;
  }
  const bool written = WriteAll(fd, header, sizeof(header)) &&
                       WriteAll(fd, payload.data(), payload.size()) &&
                       FsyncRetryingOnInterrupt(fd) == 0;
  const int write_errno = errno;
  ::close(fd);
  if (!written) {
    ::unlink(working_path_.c_str());
    SPDLOG_WARN("Checkpoint {0} could not be written to {1} (errno {2})",
                stats->generation, working_path_, write_errno);
    return false;
  }
  stats->write_ms = ElapsedMs(write_begin);

  if (::rename(working_path_.c_str(), image_path_.c_str()) != 0) {
    const int rename_errno = errno;
    ::unlink(working_path_.c_str());
    SPDLOG_WARN("Checkpoint {0} could not be published as {1} (errno {2})",
                stats->generation, image_path_, rename_errno);
    return false;
  }
  if (!FsyncDirectory(config_.work_dir)) {
    SPDLOG_WARN(
        "Checkpoint {0} was renamed but its directory entry is not durable "
        "(errno {1})",
        stats->generation, errno);
    return false;
  }
  return true;
}

EpochScanCheckpoint::Image EpochScanCheckpoint::Load(
    const std::string& work_dir) {
  Image image;
  const std::string path =
      (std::filesystem::path(work_dir) / ImageFileName()).string();
  const OpenFile file(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (file.fd < 0) {
    image.status = errno == ENOENT ? Image::Status::Absent
                                   : Image::Status::Unusable;
    image.detail = "open " + path + " (errno " + std::to_string(errno) + ")";
    return image;
  }
  const int fd = file.fd;

  auto unusable = [&](const std::string& detail) {
    image.status = Image::Status::Unusable;
    image.detail = detail;
    image.records.clear();
    return image;
  };

  struct stat file_stat{};
  if (::fstat(fd, &file_stat) < 0) return unusable("the image cannot be sized");
  if (file_stat.st_size < static_cast<off_t>(kHeaderSize)) {
    return unusable("the image is shorter than its header");
  }

  uint8_t header[kHeaderSize];
  if (!ReadAll(fd, header, sizeof(header), 0)) {
    return unusable("the image header cannot be read");
  }
  if (GetLe32(header) != kMagic) return unusable("the image magic disagrees");
  if (GetLe16(header + 4) != kVersion) {
    return unusable("the image version is not supported");
  }
  if (GetLe16(header + 6) != kFlags) {
    return unusable("the image carries unknown flags");
  }
  const uint64_t payload_size = GetLe64(header + 44);
  if (payload_size !=
      static_cast<uint64_t>(file_stat.st_size) - kHeaderSize) {
    return unusable("the image length disagrees with its header");
  }

  const EpochNumber cut_epoch = GetLe32(header + 16);
  const EpochNumber end_epoch = GetLe32(header + 20);
  const EpochNumber wal_frontier_at_publish = GetLe32(header + 24);
  // The scan ends no earlier than it began, and an image that claims
  // otherwise describes a history no run produced.
  if (cut_epoch == 0 || end_epoch < cut_epoch) {
    return unusable("the image epochs are not in order");
  }

  // Everything from here allocates in proportion to the file, and a file that
  // is damaged in its length is exactly the one that would ask for too much.
  try {
    std::vector<uint8_t> payload(payload_size);
    if (payload_size != 0 &&
        !ReadAll(fd, payload.data(), payload.size(), kHeaderSize)) {
      return unusable("the image payload cannot be read");
    }
    Crc32c crc;
    crc.Update(header, kHeaderSize - sizeof(uint32_t));
    crc.Update(payload.data(), payload.size());
    if (crc.Finish() != GetLe32(header + kHeaderSize - sizeof(uint32_t))) {
      return unusable("the image checksum does not hold");
    }

    size_t consumed = 0;
    auto handle = msgpack::unpack(
        reinterpret_cast<const char*>(payload.data()), payload.size(),
        consumed);
    handle.get().convert(image.records);
    if (consumed != payload.size()) {
      return unusable("the image payload has trailing bytes");
    }
  } catch (const std::exception& e) {
    return unusable(std::string("the image payload does not decode: ") +
                    e.what());
  } catch (...) {
    return unusable("the image payload does not decode");
  }

  image.status = Image::Status::Ok;
  image.cut_epoch = cut_epoch;
  image.end_epoch = end_epoch;
  image.wal_frontier_at_publish = wal_frontier_at_publish;
  return image;
}

}  // namespace Recovery
}  // namespace LineairDB
