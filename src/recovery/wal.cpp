#include "wal.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <msgpack.hpp>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

#include "crc32c.h"
#include "flush_trace.h"
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

/**
 * Whether these header bytes could have been left by a write of this build's own
 * frame that did not complete, as opposed to a header that is simply wrong.
 *
 * The magic, the version and the flags are constants here, and the region ahead
 * of the log holds zeroes, so an incomplete write of a header can only read as a
 * prefix of those constants followed by zeroes. A header that disagrees with them
 * in any other way was not produced by writing a frame, and the log ends before
 * whatever produced it. This says nothing about how much of the header arrived:
 * a write may also stop after the constants, inside the length or the checksum,
 * and that case is left to the checksum to catch.
 */
bool HeaderIsTornPrefix(const uint8_t* header) {
  uint8_t expected[8];
  PutLe32(expected, Wal::kMagic);
  PutLe16(expected + 4, Wal::kVersion);
  PutLe16(expected + 6, Wal::kFlags);

  size_t matched = 0;
  while (matched < sizeof(expected) && header[matched] == expected[matched]) {
    ++matched;
  }
  if (matched == sizeof(expected)) return false;
  for (size_t i = matched; i < Wal::kHeaderSize; ++i) {
    if (header[i] != 0) return false;
  }
  return true;
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
  io.pwrite = [](int fd, const void* data, size_t size, off_t offset) {
    return ::pwrite(fd, data, size, offset);
  };
  io.initialise_pwrite = io.pwrite;
  io.pread = [](int fd, void* data, size_t size, off_t offset) {
    return ::pread(fd, data, size, offset);
  };

  // A durability contract's failure behaviour can only be observed from outside
  // the process, and a real EIO cannot be arranged without privileges the test
  // does not have. The injection is armed from the environment, like the debug
  // sync points, so the binary under test is the binary that serves traffic;
  // with the variable unset, fdatasync is the bare syscall.
  const char* raw = std::getenv("LINEAIRDB_WAL_FDATASYNC_FAIL_AFTER");
  if (raw == nullptr) {
    io.fdatasync = [](int fd) { return ::fdatasync(fd); };
    return io;
  }
  errno = 0;
  char* end = nullptr;
  const long successes = std::strtol(raw, &end, 10);
  // A value that saturates rather than parses would arm the injection at a count
  // no run reaches, which reads as "the failure never happened".
  if (end == raw || *end != '\0' || errno == ERANGE || successes < 0) {
    SPDLOG_CRITICAL(
        "Invalid LINEAIRDB_WAL_FDATASYNC_FAIL_AFTER='{0}': expected a "
        "non-negative count of calls to let through",
        raw);
    exit(EXIT_FAILURE);
  }
  auto remaining = std::make_shared<std::atomic<long>>(successes);
  io.fdatasync = [remaining](int fd) -> int {
    if (remaining->fetch_sub(1) <= 0) {
      errno = EIO;
      return -1;
    }
    return ::fdatasync(fd);
  };
  return io;
}

Wal::Wal(const std::string& work_dir, WalIo io, uint64_t initial_capacity_bytes,
         size_t writer_threads)
    : io_(std::move(io)),
      initial_capacity_bytes_(initial_capacity_bytes),
      writer_threads_(writer_threads) {
  if (writer_threads_ == 0) {
    throw std::invalid_argument("WAL writer thread count must be at least one");
  }
  const std::filesystem::path directory(work_dir);
  path_ = (directory / "wal.log").string();

  std::error_code ec;
  std::filesystem::create_directory(directory, ec);
  if (ec) {
    throw std::system_error(ec, "create_directory " + work_dir);
  }

  // O_TRUNC is never used: an existing log is the only record of what was
  // acknowledged as durable. O_APPEND is never used either, and cannot be: under
  // it a pwrite ignores the offset it is given and lands at the end of the file.
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
  if (fd_ < 0 && errno == EEXIST) {
    fd_ = ::open(path_.c_str(), O_RDWR | O_CLOEXEC);
  }
  if (fd_ < 0) {
    throw std::system_error(errno, std::generic_category(), "open " + path_);
  }

  // Writing in place puts the offsets under this process's control, so a second
  // process holding the same log would overwrite frames rather than interleave
  // with them.
  if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
    const int error = errno;
    ::close(fd_);
    fd_ = -1;
    throw std::system_error(error, std::generic_category(),
                            "lock " + path_ + " exclusively");
  }

  // Every name on the way to the log is made durable here, whether or not this
  // process is the one that created it. Two processes can race to create the
  // directory or the file, and the one that loses a create can still win the lock;
  // it is then the only one left to persist what the loser was going to.
  if (FsyncRetryingOnInterrupt(fd_) != 0) {
    const int error = errno;
    ::close(fd_);
    fd_ = -1;
    throw std::system_error(error, std::generic_category(), "fsync " + path_);
  }
  {
    const std::string parent =
        directory.has_parent_path() ? directory.parent_path().string() : ".";
    int error = 0;
    if (!FsyncDirectory(directory.string(), &error) ||
        !FsyncDirectory(parent, &error)) {
      ::close(fd_);
      fd_ = -1;
      throw std::system_error(error, std::generic_category(),
                              "fsync the directories holding " + path_);
    }
  }

  struct stat file_stat{};
  if (::fstat(fd_, &file_stat) < 0) {
    const int error = errno;
    ::close(fd_);
    fd_ = -1;
    throw std::system_error(error, std::generic_category(), "fstat " + path_);
  }
  // The file's size stands in for how far an earlier incarnation got with writing
  // zeroes into it. That substitution needs one ordering from the filesystem: a
  // size that survives a crash must not run ahead of the zeroes it covers. ext4
  // with data=ordered provides it by writing back a transaction's data before
  // committing the metadata that publishes it, and that is the supported
  // configuration here. data=ordered and barriers are ext4 defaults; the data mode
  // can be changed and barriers can be disabled, and the storage stack below has to
  // honour a flush. Where the ordering does not hold, stale bytes can appear below
  // this offset instead of zeroes, and this class cannot tell: being independent of
  // the mount would take a durable marker for how far initialisation got.
  //
  // Capacity is not extended here: the region to initialise begins where the log
  // ends, and that is what ScanAndRepair establishes.
  initialised_size_ = file_stat.st_size;

  try {
    for (size_t index = 1; index < writer_threads_; ++index) {
      writer_pool_.emplace_back([this, index] { WriterLoop(index); });
    }
  } catch (...) {
    StopWriters();
    ::close(fd_);
    fd_ = -1;
    throw;
  }
}

Wal::~Wal() {
  StopWriters();
  if (fd_ >= 0) ::close(fd_);
}

WalScanResult Wal::Corrupt(const std::string& detail) {
  state_ = State::Failed;
  WalScanResult result;
  result.status = WalScanResult::Status::Corrupt;
  result.detail = detail;
  return result;
}

WalScanResult Wal::IoFailure(const std::string& operation, int error) {
  state_ = State::Failed;
  WalScanResult result;
  result.status = WalScanResult::Status::IoError;
  result.error_number = error;
  result.detail = operation;
  return result;
}

bool Wal::WriteAllAt(const uint8_t* data, size_t size, off_t offset,
                     int* error) {
  while (size != 0) {
    const size_t chunk = std::min<size_t>(size, SSIZE_MAX);
    const ssize_t written = io_.pwrite(fd_, data, chunk, offset);
    if (written > 0) {
      data += written;
      offset += written;
      size -= static_cast<size_t>(written);
      continue;
    }
    if (written < 0 && errno == EINTR) continue;
    *error = written == 0 ? EIO : errno;
    return false;
  }
  return true;
}

void Wal::WriterLoop(size_t writer_index) {
  uint64_t observed_generation = 0;
  for (;;) {
    const uint8_t* data = nullptr;
    size_t size = 0;
    off_t offset = 0;
    {
      std::unique_lock<std::mutex> lock(writer_mutex_);
      writer_work_cv_.wait(lock, [&] {
        return writer_stop_ || writer_generation_ != observed_generation;
      });
      if (writer_stop_) return;
      observed_generation = writer_generation_;

      // Divide without multiplying size by the index: WAL groups are bounded,
      // but avoiding that product keeps this correct for every size_t value.
      const size_t base = writer_size_ / writer_threads_;
      const size_t extra = writer_size_ % writer_threads_;
      const size_t begin =
          base * writer_index + std::min(writer_index, extra);
      size = base + (writer_index < extra ? 1 : 0);
      data = writer_data_ + begin;
      offset = writer_offset_ + static_cast<off_t>(begin);
    }

    int error = 0;
    const bool ok = WriteAllAt(data, size, offset, &error);
    {
      std::lock_guard<std::mutex> lock(writer_mutex_);
      if (!ok && writer_error_ == 0) writer_error_ = error;
      ++writers_completed_;
    }
    writer_done_cv_.notify_one();
  }
}

bool Wal::WriteGroupAt(const uint8_t* data, size_t size, off_t offset,
                       int* error) {
  if (writer_pool_.empty()) return WriteAllAt(data, size, offset, error);

  size_t coordinator_size = 0;
  {
    std::lock_guard<std::mutex> lock(writer_mutex_);
    writer_data_ = data;
    writer_size_ = size;
    writer_offset_ = offset;
    writers_completed_ = 0;
    writer_error_ = 0;
    ++writer_generation_;

    const size_t base = size / writer_threads_;
    const size_t extra = size % writer_threads_;
    coordinator_size = base + (extra != 0 ? 1 : 0);
  }
  writer_work_cv_.notify_all();

  int coordinator_error = 0;
  const bool coordinator_ok =
      WriteAllAt(data, coordinator_size, offset, &coordinator_error);

  std::unique_lock<std::mutex> lock(writer_mutex_);
  if (!coordinator_ok && writer_error_ == 0) {
    writer_error_ = coordinator_error;
  }
  writer_done_cv_.wait(
      lock, [this] { return writers_completed_ == writer_pool_.size(); });
  if (writer_error_ == 0) return true;
  *error = writer_error_;
  return false;
}

void Wal::StopWriters() {
  {
    std::lock_guard<std::mutex> lock(writer_mutex_);
    writer_stop_ = true;
  }
  writer_work_cv_.notify_all();
  for (auto& writer : writer_pool_) {
    if (writer.joinable()) writer.join();
  }
  writer_pool_.clear();
}

bool Wal::PreadAll(uint8_t* out, size_t size, off_t offset, int* error) const {
  while (size != 0) {
    const ssize_t got = io_.pread(fd_, out, size, offset);
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

/**
 * Writes out zeroes over `[from, to)` and persists them.
 *
 * The zeroes serve two purposes at once, which is why one function covers both
 * initialising new capacity and repairing a tail. They put the blocks in the
 * written state, so that a group flush into them persists data and nothing else.
 * And they are the mark of a region that holds no frame, which is what lets the
 * scan find the end of the log.
 *
 * The blocks are initialised by writing rather than only by posix_fallocate.
 * posix_fallocate does reserve storage, but filesystems such as ext4 may represent
 * the range as unwritten extents. Their first data write must still convert extent
 * metadata. Explicit zero writes followed by fsync move that work out of the
 * group-flush path and establish the zero tail used by recovery. Whether this is
 * faster is filesystem- and device-specific.
 */
bool Wal::WriteZeroesAndSync(off_t from, off_t to, int* error) {
  if (to <= from) return true;

  constexpr size_t kChunkSize = 1ull << 20;
  const std::vector<uint8_t> zeroes(kChunkSize, 0);
  for (off_t offset = from; offset < to;) {
    const size_t size =
        static_cast<size_t>(std::min<off_t>(kChunkSize, to - offset));
    size_t remaining = size;
    const uint8_t* data = zeroes.data();
    while (remaining != 0) {
      const ssize_t written = io_.initialise_pwrite(fd_, data, remaining, offset);
      if (written > 0) {
        data += written;
        offset += written;
        remaining -= static_cast<size_t>(written);
        continue;
      }
      if (written < 0 && errno == EINTR) continue;
      *error = written == 0 ? EIO : errno;
      return false;
    }
  }

  // The size and the blocks have to reach the device before a group relies on
  // its own fdatasync persisting data alone.
  if (FsyncRetryingOnInterrupt(fd_) != 0) {
    *error = errno;
    return false;
  }
  return true;
}

/**
 * Looks for a complete frame whose checksum holds at `offset`.
 *
 * Answers only that question, so it neither decodes the payload nor compares the
 * epoch with a neighbour: it exists to find frames a scan cannot reach by
 * following lengths, and a frame that satisfies its own checksum is one this
 * process wrote and may already have acknowledged. A payload that is empty, or an
 * epoch of zero, is not something the write path produces, and ruling those out
 * keeps the cheapest forgeries out of the candidate set.
 *
 * The checksum is taken over the payload a chunk at a time rather than over a copy
 * of it, so that a length read out of a damaged header cannot turn into an
 * allocation of that size.
 */
Wal::Probe Wal::ProbeFrameAt(off_t offset, off_t file_size, int* error) const {
  if (file_size - offset < static_cast<off_t>(kHeaderSize)) return Probe::NoFrame;

  uint8_t header[kHeaderSize];
  if (!PreadAll(header, kHeaderSize, offset, error)) return Probe::IoError;
  if (GetLe32(header) != kMagic) return Probe::NoFrame;
  if (GetLe16(header + 4) != kVersion) return Probe::NoFrame;
  if (GetLe16(header + 6) != kFlags) return Probe::NoFrame;
  if (GetLe32(header + 12) == 0) return Probe::NoFrame;

  const uint32_t payload_size = GetLe32(header + 8);
  if (payload_size == 0 || payload_size > kMaxPayloadSize) return Probe::NoFrame;
  const uint64_t frame_end =
      static_cast<uint64_t>(offset) + kHeaderSize + payload_size;
  if (frame_end > static_cast<uint64_t>(file_size)) return Probe::NoFrame;

  Crc32c crc;
  crc.Update(header, 16);
  constexpr size_t kChunkSize = 1ull << 20;
  std::vector<uint8_t> chunk(std::min<size_t>(kChunkSize, payload_size));
  off_t at = offset + static_cast<off_t>(kHeaderSize);
  size_t remaining = payload_size;
  while (remaining != 0) {
    const size_t size = std::min(chunk.size(), remaining);
    if (!PreadAll(chunk.data(), size, at, error)) return Probe::IoError;
    crc.Update(chunk.data(), size);
    at += static_cast<off_t>(size);
    remaining -= size;
  }
  return crc.Finish() == GetLe32(header + 16) ? Probe::Frame : Probe::NoFrame;
}

/**
 * Looks for a frame beginning anywhere in `(offset, search_end)`.
 *
 * Finding one is a reason to stop rather than to repair. It may be a frame that
 * was acknowledged as durable, whose own frame at `offset` was damaged afterwards.
 * It may equally belong to the same unfinished group: a group is one pwrite
 * followed by one fdatasync, and a power cut is not obliged to persist the blocks
 * of that write in order, so a later frame of it can survive while an earlier one
 * does not. Nothing in the log distinguishes the two, and only one of them is safe
 * to erase.
 *
 * The search trusts neither the broken frame's length nor its own bound for the
 * frames it finds. `search_end` bounds where a frame may begin, since a frame
 * begins with a magic and cannot begin among the zeroes past the last byte
 * written; `file_size` bounds where one may end, since a payload may itself end in
 * zeroes.
 */
Wal::Probe Wal::SearchForFrameAfter(off_t offset, off_t search_end,
                                    off_t file_size, int* error) const {
  constexpr size_t kChunkSize = 1ull << 20;
  constexpr size_t kOverlap = sizeof(uint32_t) - 1;
  std::vector<uint8_t> chunk(kChunkSize);

  for (off_t at = offset + 1; at < search_end;) {
    const size_t size =
        static_cast<size_t>(std::min<off_t>(kChunkSize, search_end - at));
    if (!PreadAll(chunk.data(), size, at, error)) return Probe::IoError;
    for (size_t i = 0; i + sizeof(uint32_t) <= size; ++i) {
      if (GetLe32(chunk.data() + i) != kMagic) continue;
      const Probe probe =
          ProbeFrameAt(at + static_cast<off_t>(i), file_size, error);
      if (probe != Probe::NoFrame) return probe;
    }
    if (size <= kOverlap) break;
    // A magic that straddles two chunks has to be whole in one of them.
    at += static_cast<off_t>(size - kOverlap);
  }
  return Probe::NoFrame;
}

/**
 * Reports the offset of the last byte in `[from, to)` that is not zero, or
 * `from - 1` when every byte is.
 */
bool Wal::FindLastNonZero(off_t from, off_t to, off_t* last_non_zero,
                          int* error) const {
  constexpr size_t kChunkSize = 1ull << 20;
  std::vector<uint8_t> chunk(kChunkSize);
  *last_non_zero = from - 1;

  for (off_t at = from; at < to; at += kChunkSize) {
    const size_t size =
        static_cast<size_t>(std::min<off_t>(kChunkSize, to - at));
    if (!PreadAll(chunk.data(), size, at, error)) return false;
    for (size_t i = size; i > 0; --i) {
      if (chunk[i - 1] != 0) {
        *last_non_zero = at + static_cast<off_t>(i) - 1;
        break;
      }
    }
  }
  return true;
}

bool Wal::EnsureCapacityFor(off_t end_of_log, size_t group_size, int* error) {
  const uint64_t limit =
      static_cast<uint64_t>(std::numeric_limits<off_t>::max());
  if (static_cast<uint64_t>(group_size) >
      limit - static_cast<uint64_t>(end_of_log)) {
    *error = EFBIG;
    return false;
  }
  // Without preallocation the group's own write is what extends the file, which
  // is the behaviour this exists to avoid. Writing zeroes over the region first
  // would cost a second write and a second sync per group.
  if (initial_capacity_bytes_ == kNoPreallocation) return true;

  const uint64_t needed = static_cast<uint64_t>(end_of_log) + group_size;
  if (needed <= static_cast<uint64_t>(initialised_size_) &&
      static_cast<uint64_t>(initialised_size_) >= initial_capacity_bytes_) {
    return true;
  }

  // Round up to a whole number of capacity units, so that a log which outgrows
  // its capacity pays for initialisation once per unit rather than once per
  // group. Computed rather than counted: a capacity of a few bytes would make
  // counting cost a step per unit on every group.
  const uint64_t units = std::max<uint64_t>(
      1, needed / initial_capacity_bytes_ +
             (needed % initial_capacity_bytes_ != 0 ? 1 : 0));
  if (units > limit / initial_capacity_bytes_) {
    *error = EFBIG;
    return false;
  }
  const uint64_t target = units * initial_capacity_bytes_;
  if (target <= static_cast<uint64_t>(initialised_size_)) return true;

  if (!WriteZeroesAndSync(initialised_size_, static_cast<off_t>(target),
                          error)) {
    return false;
  }
  initialised_size_ = static_cast<off_t>(target);
  return true;
}

/**
 * Publishes the end of the log and initialises the capacity beyond it, which is
 * the last thing a successful scan does and the point at which groups may be
 * written.
 */
WalScanResult Wal::FinishScan(WalScanResult&& result, off_t end_of_log) {
  int error = 0;
  if (!EnsureCapacityFor(end_of_log, 0, &error)) {
    return IoFailure("initialise the capacity of " + path_, error);
  }
  // Published together with Ready: a scan that could not finish leaves no offset
  // behind to be mistaken for the end of the log.
  write_offset_ = end_of_log;
  state_ = State::Ready;
  return std::move(result);
}

WalScanResult Wal::ScanAndRepair() {
  if (state_ == State::Failed) {
    return IoFailure("scan " + path_ + " after a failure", EIO);
  }

  struct stat file_stat{};
  if (::fstat(fd_, &file_stat) < 0) {
    return IoFailure("fstat " + path_, errno);
  }
  const off_t file_size = file_stat.st_size;
  initialised_size_ = std::max(initialised_size_, file_size);

  LogRecords records;
  EpochNumber frontier = 0;
  bool have_frame = false;
  off_t offset = 0;
  uint8_t header[kHeaderSize];
  std::vector<uint8_t> payload;
  // Empty while frames keep parsing; otherwise why the one at `offset` did not.
  std::string anomaly;

  while (offset < file_size) {
    if (file_size - offset < static_cast<off_t>(kHeaderSize)) {
      anomaly = "the file ends inside a frame header";
      break;
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

    const char* header_anomaly = nullptr;
    if (magic != kMagic) {
      header_anomaly = "frame magic mismatch";
    } else if (version != kVersion) {
      header_anomaly = "unsupported frame version";
    } else if (flags != kFlags) {
      header_anomaly = "unknown frame flags";
    } else if (payload_size > kMaxPayloadSize) {
      header_anomaly = "payload too large";
    }
    if (header_anomaly != nullptr) {
      if (!HeaderIsTornPrefix(header)) return Corrupt(header_anomaly);
      anomaly = header_anomaly;
      break;
    }

    const uint64_t frame_end = static_cast<uint64_t>(offset) + kHeaderSize +
                               payload_size;
    if (frame_end > static_cast<uint64_t>(file_size)) {
      anomaly = "the file ends inside a frame payload";
      break;
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
      anomaly = "frame checksum mismatch";
      break;
    }

    // A frame that satisfies its own checksum was written whole. Anything wrong
    // with it from here on cannot be blamed on an interrupted write.
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
  }

  WalScanResult result;
  result.status = WalScanResult::Status::Ok;
  result.frontier = frontier;

  if (!anomaly.empty()) {
    int error = 0;
    off_t last_non_zero = 0;
    if (!FindLastNonZero(offset, file_size, &last_non_zero, &error)) {
      return IoFailure("pread the tail of " + path_, error);
    }
    // What settles whether the bytes at `offset` end the log is whether any frame
    // survives beyond them, and that question is asked of the file rather than of
    // the broken frame's own length: a length corrupted upwards would otherwise
    // place the frames that follow inside the region a repair may erase.
    if (last_non_zero >= offset) {
      switch (SearchForFrameAfter(offset, last_non_zero + 1, file_size,
                                  &error)) {
        case Probe::Frame:
          return Corrupt(anomaly + " at offset " + std::to_string(offset) +
                         ", with a frame surviving beyond it");
        case Probe::IoError:
          // Repairing is destructive, so an unreadable candidate is a reason to
          // stop rather than a reason to believe there is nothing there.
          return IoFailure("pread past the tail of " + path_, error);
        case Probe::NoFrame:
          break;
      }
      if (!WriteZeroesAndSync(offset, last_non_zero + 1, &error)) {
        return IoFailure("zero the tail of " + path_, error);
      }
      SPDLOG_WARN(
          "Discarded an incomplete tail of {0} at offset {1} ({2}); the "
          "frontier is {3}",
          path_, static_cast<long long>(offset), anomaly, frontier);
      result.tail_truncated = true;
    }
  }

  result.records = std::move(records);
  return FinishScan(std::move(result), offset);
}

WalAppendResult Wal::AppendGroup(
    const std::map<EpochNumber, LogRecords>& buckets, EpochNumber target) {
  // Where the log ends is what a successful scan establishes, and a group whose
  // own outcome is unknown unsettles it again. Checked before the group is even
  // encoded, so that the rule holds for a caller that passes nothing eligible.
  if (state_ != State::Ready) {
    SPDLOG_CRITICAL(
        "Durability Error: a group was written to {0} while the end of the log "
        "was not established",
        path_);
    std::abort();
  }

  auto& trace                = FlushTrace::Instance();
  const bool traced          = trace.Enabled();
  const int64_t encode_begin = traced ? FlushTrace::Now() : 0;
  uint32_t encoded_epochs    = 0;
  std::vector<uint8_t> group;
  for (const auto& [epoch, records] : buckets) {
    if (epoch > target) break;
    ++encoded_epochs;
    // An empty bucket would produce a frame that recovery rejects; the caller
    // must never create one.
    assert(!records.empty());
    assert(epoch != 0);

    msgpack::sbuffer payload;
    msgpack::pack(payload, records);
    // Refused before anything is written, which leaves the end of the log known
    // and this instance usable, unlike a failure with a write behind it.
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

  if (traced) {
    trace.GroupEncode(encode_begin, FlushTrace::Now(), group.size(),
                      encoded_epochs);
  }

  if (group.empty()) return {true, 0};

  int error = 0;
  const off_t initialised_before = initialised_size_;
  if (!EnsureCapacityFor(write_offset_, group.size(), &error)) {
    state_ = State::Failed;
    return {false, error};
  }
  if (initialised_size_ != initialised_before) {
    ++extension_count_;
    SPDLOG_INFO("Extended {0} to {1} bytes ({2} extensions so far)", path_,
                static_cast<long long>(initialised_size_), extension_count_);
  }

  const int64_t write_begin = traced ? FlushTrace::Now() : 0;
  if (!WriteGroupAt(group.data(), group.size(), write_offset_, &error)) {
    state_ = State::Failed;
    return {false, error};
  }
  if (traced) trace.GroupWrite(write_begin, FlushTrace::Now());
  // The records are in the page cache and not yet on the device: a Sync commit
  // waiting on this group must not have been acknowledged when this point is
  // reached.
  LINEAIRDB_DEBUG_SYNC("wal.before_fdatasync");

  const int64_t sync_begin = traced ? FlushTrace::Now() : 0;
  int rc;
  do {
    rc = io_.fdatasync(fd_);
  } while (rc < 0 && errno == EINTR);
  if (rc < 0) {
    const int failure = errno;
    state_ = State::Failed;
    return {false, failure};
  }
  if (traced) trace.GroupSync(sync_begin, FlushTrace::Now());

  write_offset_ += static_cast<off_t>(group.size());
  // Without preallocation the group carried the file's size with it.
  initialised_size_ = std::max(initialised_size_, write_offset_);
  return {true, 0};
}

}  // namespace Recovery
}  // namespace LineairDB
