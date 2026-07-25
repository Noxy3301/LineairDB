#include "wal.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <filesystem>
#include <msgpack.hpp>
#include <system_error>
#include <utility>
#include <vector>

#include "crc32c.h"
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
  const std::filesystem::path directory(work_dir);
  path_ = (directory / "wal.log").string();

  std::error_code ec;
  const bool created_directory = std::filesystem::create_directory(directory, ec);
  if (ec) {
    throw std::system_error(ec, "create_directory " + work_dir);
  }
  if (created_directory) {
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
  bool created_file = false;
  fd_ = ::open(path_.c_str(),
               O_RDWR | O_CREAT | O_EXCL | O_APPEND | O_CLOEXEC, 0644);
  if (fd_ >= 0) {
    created_file = true;
  } else if (errno == EEXIST) {
    fd_ = ::open(path_.c_str(), O_RDWR | O_APPEND | O_CLOEXEC);
  }
  if (fd_ < 0) {
    throw std::system_error(errno, std::generic_category(), "open " + path_);
  }

  if (created_file) {
    if (FsyncRetryingOnInterrupt(fd_) != 0) {
      const int error = errno;
      ::close(fd_);
      fd_ = -1;
      throw std::system_error(error, std::generic_category(),
                              "fsync " + path_);
    }
    int error = 0;
    if (!FsyncDirectory(directory.string(), &error)) {
      ::close(fd_);
      fd_ = -1;
      throw std::system_error(error, std::generic_category(),
                              "fsync directory of " + path_);
    }
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
  if (FsyncRetryingOnInterrupt(fd_) != 0) {
    return IoFailure("fsync after ftruncate " + path_, errno);
  }
  SPDLOG_WARN(
      "Discarded an incomplete tail of {0} at offset {1}; the frontier is {2}",
      path_, static_cast<long long>(last_good), frontier);

  WalScanResult result;
  result.status = WalScanResult::Status::Ok;
  result.frontier = frontier;
  result.records = std::move(records);
  result.tail_truncated = true;
  return result;
}

WalScanResult Wal::ScanAndRepair() {
  struct stat file_stat{};
  if (::fstat(fd_, &file_stat) < 0) {
    return IoFailure("fstat " + path_, errno);
  }
  const off_t file_size = file_stat.st_size;

  LogRecords records;
  EpochNumber frontier = 0;
  bool have_frame = false;
  off_t offset = 0;
  off_t last_good = 0;
  uint8_t header[kHeaderSize];
  std::vector<uint8_t> payload;

  while (offset < file_size) {
    if (file_size - offset < static_cast<off_t>(kHeaderSize)) {
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
      return TruncateTail(last_good, frontier, std::move(records));
    }

    payload.resize(payload_size);
    if (payload_size != 0 &&
        !PreadAll(payload.data(), payload_size, offset + kHeaderSize, &error)) {
      return IoFailure("pread payload of " + path_, error);
    }

    Crc32c crc;
    crc.Update(header, 16);
    crc.Update(payload.data(), payload.size());
    if (crc.Finish() != stored_crc) {
      // Only the last frame in the file can be blamed on an interrupted
      // append; anything earlier means a frame we may already have
      // acknowledged is unreadable.
      if (frame_end == static_cast<uint64_t>(file_size)) {
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

  WalScanResult result;
  result.status = WalScanResult::Status::Ok;
  result.frontier = frontier;
  result.records = std::move(records);
  return result;
}

WalAppendResult Wal::AppendGroup(
    const std::map<EpochNumber, LogRecords>& buckets, EpochNumber target) {
  std::vector<uint8_t> group;
  for (const auto& [epoch, records] : buckets) {
    if (epoch > target) break;
    // An empty bucket would produce a frame that recovery rejects; the caller
    // must never create one.
    assert(!records.empty());
    assert(epoch != 0);

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

  if (group.empty()) return {true, 0};

  int error = 0;
  if (!WriteAll(group.data(), group.size(), &error)) {
    return {false, error};
  }
  int rc;
  do {
    rc = io_.fdatasync(fd_);
  } while (rc < 0 && errno == EINTR);
  if (rc < 0) return {false, errno};

  return {true, 0};
}

}  // namespace Recovery
}  // namespace LineairDB
