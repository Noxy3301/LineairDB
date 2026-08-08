#ifndef LINEAIRDB_RECOVERY_WAL_H
#define LINEAIRDB_RECOVERY_WAL_H

#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <string>

#include "log_record.h"
#include "types/definitions.h"

namespace LineairDB {
namespace Recovery {

/**
 * @brief Result of scanning the WAL at startup.
 *
 * @details
 * `Ok` means every frame up to the end of the log was complete and
 * consistent, and `frontier` is the epoch of the last one; a log with no
 * frames yields frontier 0. An incomplete or checksum-broken frame at the
 * end of the log is repaired rather than reported: the bytes it left behind
 * are overwritten with zeroes, `tail_truncated` is set, and the scan still
 * succeeds. Repair requires that no intact frame survives beyond the
 * damage: one that does is taken as evidence that the damage sits in a
 * region older appends already synced.
 *
 * `Corrupt` means damage the scan will not repair:
 *   - a checksum-broken complete frame that is not the last bytes of the
 *     log, whatever follows it;
 *   - a damaged tail with an intact frame surviving beyond it, or one the
 *     search cannot classify within its I/O budget;
 *   - a magic, version, flags or length anomaly whose header does not read
 *     as a torn prefix of the frame constants (one that does is treated as
 *     an interrupted write and repaired instead);
 *   - and, in a complete checksum-valid frame, a regressed or zero epoch or
 *     a payload anomaly.
 * Nothing is zeroed and the caller must fail-stop: the log may be missing
 * records that were already acknowledged as durable.
 *
 * @note Ambiguity is resolved toward fail-stop: a torn group whose pages
 * persisted out of order, or torn user data that embeds a byte-exact intact
 * frame, can fail-stop a repairable log, and manually zeroing the damaged
 * tail then recovers it. The reverse never happens: repair never discards a
 * frame that another intact frame vouches for, and a tail the search cannot
 * classify within its I/O budget also fail-stops.
 */
struct WalScanResult {
  enum class Status { Ok, Corrupt, IoError };

  Status status{Status::Ok};
  EpochNumber frontier{0};
  LogRecords records;
  bool tail_truncated{false};
  int error_number{0};
  std::string detail;
  /** Frames the scan verified but did not decode, and what they held. */
  size_t frames_skipped{0};
  uint64_t bytes_skipped{0};
};

struct WalAppendResult {
  bool ok{true};
  int error_number{0};
};

/**
 * @brief Seam for the syscalls the append and capacity paths use, letting a
 * test inject a write, sync, initialisation or read failure.
 * @details `pwrite` and `fdatasync` carry a group; `initialise_pwrite`
 * carries the zeroes that reserve capacity, kept separate so a capacity
 * failure cannot consume an injection aimed at a group; `pread` is what
 * the scan and the tail search read through.
 * @note Everything else (open, flock, fstat, and the fsync that follows a
 * zero write) is always the real syscall.
 * @note LINEAIRDB_WAL_FDATASYNC_FAIL_AFTER=<count> makes the fdatasync that
 * Posix() returns let that many calls through and fail every later call with
 * EIO. Each Posix() call creates one counter, shared by every copy of the
 * WalIo it returned. A value that is not decimal digits, or that does not
 * fit in a long, stops startup.
 */
struct WalIo {
  std::function<ssize_t(int, const void*, size_t, off_t)> pwrite;
  std::function<int(int)> fdatasync;
  std::function<ssize_t(int, const void*, size_t, off_t)> initialise_pwrite;
  std::function<ssize_t(int, void*, size_t, off_t)> pread;

  static WalIo Posix();
};

/**
 * @brief The single write-ahead log: one file of epoch frames, written by
 * one flusher.
 *
 * @details
 * Frames are written in place, at an offset the instance tracks, into a
 * region already written out with zeroes, so that a group's fdatasync
 * persists data and not the size, allocation or extent-state metadata a
 * growing file drags in (see Config::wal_initial_capacity_bytes).
 *
 * Two consequences run through the rest of this class. The end of the log
 * is not the end of the file: it is where the zeroes begin, which is why
 * nothing may be written before ScanAndRepair has found it. And the zeroes
 * ahead of the log are an invariant, not an accident: they are what makes
 * an interrupted write recognisable, so a repair restores them.
 *
 * Frame layout, little-endian:
 *   magic(u32) version(u16) flags(u16) payload_len(u32) epoch(u32) crc32c(u32)
 *   payload
 *
 * The checksum covers the header up to but excluding the crc field, plus
 * the payload, so a bit flip in the epoch or length is detected too. The
 * payload is the msgpack encoding of a non-empty record list carrying the
 * frame's epoch, and frame epochs are non-decreasing. No frame ever carries
 * an empty record list; the caller keeps empty buckets out of AppendGroup,
 * which refuses them rather than skipping them.
 *
 * @note An exclusive flock keeps out a second process; it does not make a
 * second concurrent appender inside this process defined.
 */
class Wal {
 public:
  /**
   * @brief Opens the log and takes an exclusive lock on it.
   * @details Nothing is allocated and nothing is read yet: ScanAndRepair
   * does both, in that order, because the region to initialise is the one
   * the scan finds to be past the log.
   * @param[in] initial_capacity_bytes How much is made writable in place at
   * a time. It is a granularity rather than a limit: a log that outgrows it
   * is extended by the same amount again, at the price of one synchronous
   * initialisation. `kNoPreallocation` leaves the file to grow as it is
   * written, which is what the Volatile contract is given: it writes no
   * record at all, so reserving would occupy the space for nothing.
   */
  Wal(const std::string& work_dir, WalIo io = WalIo::Posix(),
      uint64_t initial_capacity_bytes = kDefaultCapacityBytes);
  ~Wal();

  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;

  /**
   * @brief Reads the log from the beginning, repairs an interrupted tail,
   * and initialises the capacity beyond it.
   * @return See WalScanResult; `Ok` carries the frontier and the records.
   * @note Must succeed before the first append: it is what locates the end
   * of the log, and until the bytes of an interrupted write are overwritten
   * with zeroes a later shorter group would leave them behind as a frame
   * the next scan cannot place. A scan run after this instance has already
   * failed does not retry; it reports the failure again.
   *
   * A frame at or below `min_epoch` is verified and counted but not decoded,
   * for a caller that already holds the state those frames would rebuild. The
   * checksum is still taken: the frontier and the end of the log come from
   * every frame, whether or not its records are wanted.
   */
  WalScanResult ScanAndRepair(EpochNumber min_epoch = 0);

  /**
   * @brief Appends one frame per bucket whose epoch is at or below `target`,
   * in epoch order, as one group write followed by one fdatasync.
   * @param[in] buckets Records grouped by their commit epoch. Buckets above
   * `target` are ignored and stay the caller's to carry forward.
   * @param[in] target The highest epoch this call may write.
   * @return Failure is returned without having advanced anything the caller
   * may publish. A bucket that would produce a frame the scan rejects
   * (empty, epoch zero, an epoch below the log's frontier, a record epoch
   * disagreeing with its bucket) fails with EINVAL before anything is
   * written, as does a payload too large to be framed with EOVERFLOW; both
   * leave the instance usable.
   * @note Calling this before a successful scan, or after a failure has
   * left the end of the log unknown, fail-stops the process rather than
   * returning: there is nothing trustworthy to append at.
   */
  WalAppendResult AppendGroup(const std::map<EpochNumber, LogRecords>& buckets,
                             EpochNumber target);

  const std::string& path() const { return path_; }

  /** @brief Offset one past the last frame, which is where the next group
   * lands. */
  off_t write_offset() const { return write_offset_; }

  /**
   * @brief The epoch of the last frame actually on disk, safe to read from a
   * thread other than the one that owns this instance between StartFlusher
   * and the join.
   * @details This moves only when a frame is written: an epoch that closed
   * without a record advances the durable epoch a commit waits on, but it
   * advances this not at all, which is what a caller needs from it when the
   * question is what the log itself can be trusted to still hold after a
   * crash.
   */
  EpochNumber frontier() const {
    return frontier_.load(std::memory_order_seq_cst);
  }

  /**
   * @brief How many times capacity had to be extended.
   * @details Extension is synchronous and writes out a whole new region, so
   * a measurement that means to see the cost of a group flush alone has to
   * report this as zero.
   */
  size_t extension_count() const { return extension_count_; }

  static constexpr uint32_t kMagic = 0x4c57414c;  // "LAWL"
  static constexpr uint16_t kVersion = 1;
  static constexpr uint16_t kFlags = 0;
  static constexpr size_t kHeaderSize = 20;
  static constexpr uint32_t kMaxPayloadSize = 256u * 1024u * 1024u;
  static constexpr uint64_t kDefaultCapacityBytes = 64ull * 1024ull * 1024ull;
  static constexpr uint64_t kNoPreallocation = 0;

 private:
  enum class State { Unscanned, Ready, Failed };
  /**
   * @brief Outcome of looking for a frame at one offset.
   * @details A read that fails is its own answer and never a "no": what
   * follows a negative answer is a repair that erases bytes, so an
   * unreadable candidate has to stop the scan instead. Exhausting the
   * probe's I/O budget is the same kind of non-answer: the search stops
   * rather than guessing that nothing was there.
   */
  enum class Probe { NoFrame, Frame, IoError, Undecidable };

  WalScanResult Corrupt(const std::string& detail);
  WalScanResult IoFailure(const std::string& operation, int error);
  WalScanResult FinishScan(WalScanResult&& result, off_t end_of_log);
  Probe ProbeFrameAt(off_t offset, off_t file_size, uint64_t* io_budget,
                     int* error) const;
  Probe SearchForFrameAfter(off_t offset, off_t search_end, off_t file_size,
                            int* error) const;
  bool FindLastNonZero(off_t from, off_t to, off_t* last_non_zero,
                       int* error) const;
  bool EnsureCapacityFor(off_t end_of_log, size_t group_size, int* error);
  bool WriteZeroesAndSync(off_t from, off_t to, int* error);
  bool WriteAllAt(const uint8_t* data, size_t size, off_t offset, int* error);
  bool PreadAll(uint8_t* out, size_t size, off_t offset, int* error) const;

  // Cumulative bytes SearchForFrameAfter and the ProbeFrameAt calls it makes
  // may read while looking for a survivor past one damaged tail. Bounds the
  // search: each false-positive magic match costs a fresh checksum re-read,
  // and without a bound that cost is unbounded in the number of candidates.
  static constexpr uint64_t kProbeBudget = 1024ull * 1024ull * 1024ull;

  std::string path_;
  WalIo io_;
  int fd_{-1};
  uint64_t initial_capacity_bytes_;
  State state_{State::Unscanned};
  off_t write_offset_{0};
  std::atomic<EpochNumber> frontier_{0};  // read cross-thread through frontier()
  /**
   * @brief The file's size, which under preallocation is also the offset
   * below which every block is allocated and holds written-out zeroes.
   */
  off_t initialised_size_{0};
  size_t extension_count_{0};
};

}  // namespace Recovery
}  // namespace LineairDB

#endif /* LINEAIRDB_RECOVERY_WAL_H */
