#include "wal.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <msgpack.hpp>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

#include "crc32c.h"
#include "util/debug_sync.hpp"
#include "util/logger.hpp"

namespace LineairDB {
namespace Recovery {

namespace {

void PutLe16(uint8_t* out, uint16_t value) {
  out[0] = static_cast<uint8_t>(value & 0xffu);
  out[1] = static_cast<uint8_t>((value >> 8) & 0xffu);
}

void PutLe32(uint8_t* out, uint32_t value) {
  out[0] = static_cast<uint8_t>(value & 0xffu);
  out[1] = static_cast<uint8_t>((value >> 8) & 0xffu);
  out[2] = static_cast<uint8_t>((value >> 16) & 0xffu);
  out[3] = static_cast<uint8_t>((value >> 24) & 0xffu);
}

uint16_t GetLe16(const uint8_t* in) {
  return static_cast<uint16_t>(in[0]) |
         static_cast<uint16_t>(static_cast<uint16_t>(in[1]) << 8);
}

uint32_t GetLe32(const uint8_t* in) {
  return static_cast<uint32_t>(in[0]) |
         (static_cast<uint32_t>(in[1]) << 8) |
         (static_cast<uint32_t>(in[2]) << 16) |
         (static_cast<uint32_t>(in[3]) << 24);
}

int FsyncRetryingOnInterrupt(int fd) {
  int rc;
  do {
    rc = ::fsync(fd);
  } while (rc < 0 && errno == EINTR);
  return rc;
}

// Persists a directory entry: a file's own fsync does not make its name
// durable.
bool FsyncDirectory(const std::string& directory, int* error) {
  const int dir_fd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (dir_fd < 0) {
    *error = errno;
    return false;
  }
  const bool ok = FsyncRetryingOnInterrupt(dir_fd) == 0;
  if (!ok) *error = errno;
  ::close(dir_fd);
  return ok;
}

}  // namespace

WalIo WalIo::Posix() {
  WalIo io;
  io.write = [](int fd, const void* data, size_t size) {
    return ::write(fd, data, size);
  };
  io.fdatasync = [](int fd) { return ::fdatasync(fd); };
  return io;
}

Wal::Wal(const std::string& work_dir, WalIo io) : io_(std::move(io)) {
  // Strip a trailing separator first; parent_path() below must name the
  // true parent, not the directory itself.
  std::filesystem::path directory(work_dir);
  if (!directory.has_filename()) directory = directory.parent_path();
  path_ = (directory / "wal.log").string();

  std::error_code ec;
  std::filesystem::create_directory(directory, ec);
  if (ec) {
    throw std::system_error(ec, "create_directory " + work_dir);
  }
  // Unconditionally: a constructor retried after an earlier fsync failure
  // would otherwise find the entries already created and skip making them
  // durable.
  {
    const std::string parent =
        directory.has_parent_path() ? directory.parent_path().string() : ".";
    int error = 0;
    if (!FsyncDirectory(parent, &error)) {
      throw std::system_error(error, std::generic_category(),
                              "fsync parent of " + work_dir);
    }
  }

  // O_TRUNC is never used: an existing log is the only record of what was
  // acknowledged as durable.
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
  if (fd_ < 0) {
    throw std::system_error(errno, std::generic_category(), "open " + path_);
  }

  if (FsyncRetryingOnInterrupt(fd_) != 0) {
    const int error = errno;
    ::close(fd_);
    fd_ = -1;
    throw std::system_error(error, std::generic_category(), "fsync " + path_);
  }
  int error = 0;
  if (!FsyncDirectory(directory.string(), &error)) {
    ::close(fd_);
    fd_ = -1;
    throw std::system_error(error, std::generic_category(),
                            "fsync directory of " + path_);
  }
}

Wal::~Wal() {
  if (fd_ >= 0) ::close(fd_);
}

WalScanResult Wal::Corrupt(const std::string& detail) const {
  WalScanResult result;
  result.status = WalScanResult::Status::Corrupt;
  result.detail = detail;
  return result;
}

WalScanResult Wal::IoFailure(const std::string& operation, int error) const {
  WalScanResult result;
  result.status = WalScanResult::Status::IoError;
  result.error_number = error;
  result.detail = operation;
  return result;
}

bool Wal::WriteAll(const uint8_t* data, size_t size, int* error) {
  while (size != 0) {
    const ssize_t written = io_.write(fd_, data, size);
    if (written > 0) {
      data += written;
      size -= static_cast<size_t>(written);
      continue;
    }
    if (written < 0 && errno == EINTR) continue;
    *error = written == 0 ? EIO : errno;
    return false;
  }
  return true;
}

bool Wal::PreadAll(uint8_t* out, size_t size, off_t offset, int* error) const {
  while (size != 0) {
    const ssize_t got = ::pread(fd_, out, size, offset);
    if (got > 0) {
      out += got;
      offset += got;
      size -= static_cast<size_t>(got);
      continue;
    }
    if (got < 0 && errno == EINTR) continue;
    // A short read below the size fstat reported means the file changed under
    // us, which this design does not allow.
    *error = got == 0 ? EIO : errno;
    return false;
  }
  return true;
}

WalScanResult Wal::TruncateTail(off_t last_good, EpochNumber frontier,
                                LogRecords&& records) {
  if (::ftruncate(fd_, last_good) < 0) {
    return IoFailure("ftruncate " + path_, errno);
  }
  // The truncation exists but is not durable yet. The flag survives this
  // scan, so a retried scan that finds nothing left to repair still syncs
  // before it may report Ok.
  repair_unsynced_ = true;
  if (FsyncRetryingOnInterrupt(fd_) != 0) {
    return IoFailure("fsync after ftruncate " + path_, errno);
  }
  repair_unsynced_ = false;
  SPDLOG_WARN(
      "Discarded an unreadable tail of {0} at offset {1}; the frontier is {2}",
      path_, static_cast<long long>(last_good), frontier);

  WalScanResult result;
  result.status = WalScanResult::Status::Ok;
  result.frontier = frontier;
  result.records = std::move(records);
  result.tail_truncated = true;
  return result;
}

// An intact frame past the damage is taken as evidence that the damage sits
// in a region older appends already synced, and repair would then discard
// acknowledged frames. Byte-scans for the magic and accepts a candidate only
// if its header is sane and its checksum verifies within the file. Every
// undecided path resolves toward fail-stop or an explicit error, never
// toward repair: a read failure is reported rather than treated as absence,
// and a read that would take the probe past its I/O budget is not issued;
// the probe reports the tail as undecidable instead, even in the middle of
// a candidate. Intact means header-sane and checksum-valid only; the
// verdict asserts no more than that the bytes parse as a frame. A
// candidate-free suffix costs one linear read, the same order as the scan
// that precedes the probe; the budget bounds the superlinear candidate
// re-reads.
Wal::TailProbe Wal::ProbeTailForAnIntactFrame(off_t damage, off_t file_size,
                                              int* error) const {
  constexpr uint64_t kWindowSize = 4u * 1024u * 1024u;
  constexpr uint64_t kProbeBudget = 1024u * 1024u * 1024u;
  uint64_t spent = 0;
  std::vector<uint8_t> window;
  std::vector<uint8_t> chunk;

  // 0 = no, 1 = yes, -1 = read failure (*error holds errno), -2 = budget.
  const auto frame_is_intact = [&](uint64_t at, const uint8_t header[]) {
    const uint32_t payload_size = GetLe32(header + 8);
    if (payload_size > kMaxPayloadSize) return 0;
    const uint64_t end = at + kHeaderSize + payload_size;
    if (end > static_cast<uint64_t>(file_size)) return 0;
    Crc32c crc;
    crc.Update(header, 16);
    chunk.resize(static_cast<size_t>(
        std::min<uint64_t>(payload_size, kWindowSize)));
    uint64_t cursor = at + kHeaderSize;
    uint64_t remaining = payload_size;
    while (remaining != 0) {
      const size_t step = static_cast<size_t>(
          std::min<uint64_t>(remaining, chunk.size()));
      if (spent + step > kProbeBudget) return -2;
      if (!PreadAll(chunk.data(), step, static_cast<off_t>(cursor), error)) {
        return -1;
      }
      crc.Update(chunk.data(), step);
      cursor += step;
      remaining -= step;
      spent += step;
    }
    return crc.Finish() == GetLe32(header + 16) ? 1 : 0;
  };

  // Windows overlap by one byte less than a header so that no candidate
  // straddling a boundary is missed. Offsets are unsigned to keep the
  // arithmetic defined for any file size.
  uint64_t window_base = static_cast<uint64_t>(damage) + 1;
  while (window_base + kHeaderSize <= static_cast<uint64_t>(file_size)) {
    const size_t want = static_cast<size_t>(std::min<uint64_t>(
        kWindowSize, static_cast<uint64_t>(file_size) - window_base));
    window.resize(want);
    if (spent + want > kProbeBudget) return TailProbe::Undecidable;
    if (!PreadAll(window.data(), want, static_cast<off_t>(window_base),
                  error)) {
      return TailProbe::IoError;
    }
    spent += want;
    for (size_t at = 0; at + kHeaderSize <= want; ++at) {
      const uint8_t* header = window.data() + at;
      if (GetLe32(header) != kMagic) continue;
      if (GetLe16(header + 4) != kVersion) continue;
      if (GetLe16(header + 6) != kFlags) continue;
      const int verdict = frame_is_intact(window_base + at, header);
      if (verdict == -2) return TailProbe::Undecidable;
      if (verdict == -1) return TailProbe::IoError;
      if (verdict > 0) return TailProbe::IntactFrame;
    }
    if (want < kWindowSize) break;
    window_base += want - (kHeaderSize - 1);
  }
  return TailProbe::NoIntactFrame;
}

WalScanResult Wal::ScanAndRepair() {
  auto scan = [&]() -> WalScanResult {
  struct stat file_stat{};
  if (::fstat(fd_, &file_stat) < 0) {
    return IoFailure("fstat " + path_, errno);
  }
  const off_t file_size = file_stat.st_size;

  // Every undecided probe outcome refuses the repair; only a definite
  // absence of intact frames past the damage lets a truncation proceed.
  const auto probe_refusal = [&](off_t damage, WalScanResult* refusal) {
    int probe_error = 0;
    switch (ProbeTailForAnIntactFrame(damage, file_size, &probe_error)) {
      case TailProbe::IntactFrame:
        *refusal = Corrupt("an intact frame follows the damage");
        return true;
      case TailProbe::Undecidable:
        *refusal = Corrupt("the tail cannot be classified within the probe budget");
        return true;
      case TailProbe::IoError:
        *refusal = IoFailure("probe tail of " + path_, probe_error);
        return true;
      case TailProbe::NoIntactFrame:
        break;
    }
    return false;
  };

  LogRecords records;
  EpochNumber frontier = 0;
  bool have_frame = false;
  off_t offset = 0;
  off_t last_good = 0;
  uint8_t header[kHeaderSize];
  std::vector<uint8_t> payload;

  while (offset < file_size) {
    if (file_size - offset < static_cast<off_t>(kHeaderSize)) {
      WalScanResult refusal;
      if (probe_refusal(offset, &refusal)) return refusal;
      return TruncateTail(last_good, frontier, std::move(records));
    }
    int error = 0;
    if (!PreadAll(header, kHeaderSize, offset, &error)) {
      return IoFailure("pread header of " + path_, error);
    }

    const uint32_t magic = GetLe32(header);
    const uint16_t version = GetLe16(header + 4);
    const uint16_t flags = GetLe16(header + 6);
    const uint32_t payload_size = GetLe32(header + 8);
    const EpochNumber epoch = GetLe32(header + 12);
    const uint32_t stored_crc = GetLe32(header + 16);

    if (magic != kMagic) return Corrupt("frame magic mismatch");
    if (version != kVersion) return Corrupt("unsupported frame version");
    if (flags != kFlags) return Corrupt("unknown frame flags");
    if (payload_size > kMaxPayloadSize) return Corrupt("payload too large");

    const uint64_t frame_end = static_cast<uint64_t>(offset) + kHeaderSize +
                               payload_size;
    if (frame_end > static_cast<uint64_t>(file_size)) {
      // A corrupted length can claim the rest of the file; an intact frame
      // inside the claimed range is evidence against a tear.
      WalScanResult refusal;
      if (probe_refusal(offset, &refusal)) return refusal;
      return TruncateTail(last_good, frontier, std::move(records));
    }

    try {
      payload.resize(payload_size);
    } catch (const std::bad_alloc&) {
      return IoFailure("allocate payload of " + path_, ENOMEM);
    }
    if (payload_size != 0 &&
        !PreadAll(payload.data(), payload_size, offset + kHeaderSize, &error)) {
      return IoFailure("pread payload of " + path_, error);
    }

    Crc32c crc;
    crc.Update(header, 16);
    crc.Update(payload.data(), payload.size());
    if (crc.Finish() != stored_crc) {
      // A checksum break anywhere but the file's last bytes is not
      // repaired: it sits in an already-synced region, or is a torn group
      // persisted out of order, and ambiguity resolves toward fail-stop.
      if (frame_end == static_cast<uint64_t>(file_size)) {
        WalScanResult refusal;
        if (probe_refusal(offset, &refusal)) return refusal;
        return TruncateTail(last_good, frontier, std::move(records));
      }
      return Corrupt("frame checksum mismatch before the end of the file");
    }

    if (have_frame && epoch < frontier) return Corrupt("frame epoch regressed");
    if (epoch == 0) return Corrupt("frame epoch is zero");

    LogRecords decoded;
    try {
      size_t consumed = 0;
      auto handle = msgpack::unpack(
          reinterpret_cast<const char*>(payload.data()), payload.size(),
          consumed);
      handle.get().convert(decoded);
      if (consumed != payload.size()) {
        return Corrupt("frame payload has trailing bytes");
      }
    } catch (const std::bad_alloc&) {
      throw;  // Resource exhaustion is the outer handler's IoError, not
              // evidence of corruption.
    } catch (const std::exception& e) {
      return Corrupt(std::string("frame payload does not decode: ") + e.what());
    } catch (...) {
      return Corrupt("frame payload does not decode");
    }
    if (decoded.empty()) return Corrupt("frame carries no record");
    for (const auto& record : decoded) {
      if (record.epoch != epoch) {
        return Corrupt("record epoch disagrees with its frame");
      }
    }

    records.insert(records.end(), std::make_move_iterator(decoded.begin()),
                   std::make_move_iterator(decoded.end()));
    frontier = epoch;
    have_frame = true;
    offset = static_cast<off_t>(frame_end);
    last_good = offset;
  }

  WalScanResult complete;
  complete.status = WalScanResult::Status::Ok;
  complete.frontier = frontier;
  complete.records = std::move(records);
  return complete;
  };

  WalScanResult result;
  try {
    result = scan();
  } catch (const std::bad_alloc&) {
    result = IoFailure("allocate while scanning " + path_, ENOMEM);
  }
  if (result.status == WalScanResult::Status::Ok && repair_unsynced_) {
    if (FsyncRetryingOnInterrupt(fd_) != 0) {
      result = IoFailure("fsync of an earlier repair of " + path_, errno);
    } else {
      repair_unsynced_ = false;
      // The truncation this call made durable came from an earlier failed
      // call on this instance; it is this scan's repair to report.
      result.tail_truncated = true;
    }
  }
  if (result.status == WalScanResult::Status::Ok) {
    scanned_ = true;
    frontier_ = result.frontier;
  } else {
    scanned_ = false;
  }
  return result;
}

WalAppendResult Wal::AppendGroup(
    const std::map<EpochNumber, LogRecords>& buckets, EpochNumber target) {
  // The file may end in a torn frame: before the scan removes one, and after
  // a failed write or sync leaves one, appending would turn the tear into
  // mid-file corruption.
  if (!scanned_) return {false, EINVAL};
  if (append_failed_) return {false, EIO};

  std::vector<uint8_t> group;
  EpochNumber last_encoded = frontier_;
  try {
  for (const auto& [epoch, records] : buckets) {
    if (epoch > target) break;
    // A bucket the scan would reject is refused before anything is written.
    if (records.empty() || epoch == 0) return {false, EINVAL};
    if (epoch < frontier_) return {false, EINVAL};
    for (const auto& record : records) {
      if (record.epoch != epoch) return {false, EINVAL};
    }
    last_encoded = epoch;

    msgpack::sbuffer payload;
    msgpack::pack(payload, records);
    if (payload.size() > kMaxPayloadSize ||
        payload.size() > static_cast<size_t>(UINT32_MAX)) {
      return {false, EOVERFLOW};
    }

    const size_t frame_offset = group.size();
    group.resize(frame_offset + kHeaderSize + payload.size());
    uint8_t* frame = group.data() + frame_offset;
    PutLe32(frame, kMagic);
    PutLe16(frame + 4, kVersion);
    PutLe16(frame + 6, kFlags);
    PutLe32(frame + 8, static_cast<uint32_t>(payload.size()));
    PutLe32(frame + 12, epoch);
    std::memcpy(frame + kHeaderSize, payload.data(), payload.size());

    Crc32c crc;
    crc.Update(frame, 16);
    crc.Update(payload.data(), payload.size());
    PutLe32(frame + 16, crc.Finish());
  }
  } catch (const std::bad_alloc&) {
    // Nothing was written; the caller may retry.
    return {false, ENOMEM};
  } catch (const std::length_error&) {
    // The aggregated group outgrew the buffer's limits before any write.
    return {false, EOVERFLOW};
  }

  if (group.empty()) return {true, 0};

  int error = 0;
  if (!WriteAll(group.data(), group.size(), &error)) {
    append_failed_ = true;
    return {false, error};
  }
  // The records are written but not yet known durable: a Sync commit waiting
  // on this group must not have been acknowledged when this point is reached.
  LINEAIRDB_DEBUG_SYNC("wal.before_fdatasync");

  int rc;
  do {
    rc = io_.fdatasync(fd_);
  } while (rc < 0 && errno == EINTR);
  if (rc < 0) {
    append_failed_ = true;
    return {false, errno};
  }

  frontier_ = last_encoded;
  return {true, 0};
}

}  // namespace Recovery
}  // namespace LineairDB
