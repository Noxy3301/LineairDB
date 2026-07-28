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
 * Result of scanning the WAL at startup.
 *
 * `Ok` means every frame up to the end of the log was complete and consistent,
 * and `frontier` is the epoch of the last one; a log with no frames yields
 * frontier 0. An incomplete or checksum-broken frame at the end of the log is
 * repaired rather than reported: the bytes it left behind are overwritten with
 * zeroes, `tail_truncated` is set, and the scan still succeeds.
 *
 * `Corrupt` means a state that cannot be repaired without risking records that may
 * already have been acknowledged: a frame that does not parse with another frame
 * surviving beyond it, a non-monotonic epoch, or a header whose constant fields
 * arrived whole and disagree. Nothing is rewritten and the caller must fail-stop.
 *
 * The failure this distinction covers is a write cut short by a crash. Such a
 * frame was never acknowledged, because an acknowledgement follows the fdatasync
 * that would have completed it, which is what makes discarding it safe.
 *
 * Two things fall outside it. Media that loses or alters a frame after its
 * fdatasync returned: a last frame damaged that way reads as an interrupted write
 * and is discarded, and it may have been acknowledged. And an ordinary crash that
 * persists the blocks of one group out of order, leaving a later frame of it
 * behind an earlier one that is missing: that reads as damage rather than as a
 * tail, and startup is refused. The checksums here detect damage; they do not
 * authenticate, so a writer that can alter the log at will is outside this too.
 */
struct WalScanResult {
  enum class Status { Ok, Corrupt, IoError };

  Status status{Status::Ok};
  EpochNumber frontier{0};
  LogRecords records;
  bool tail_truncated{false};
  int error_number{0};
  std::string detail;
  /**
   * Frames the scan did not decode, and what they held. Verified by checksum
   * unless the scan hopped over it by header alone; see ScanAndRepair.
   */
  size_t frames_skipped{0};
  uint64_t bytes_skipped{0};
};

struct WalAppendResult {
  bool ok{true};
  int error_number{0};
};

/**
 * Seam for the syscalls that can fail on a write, so that a test can arrange the
 * failure. `pwrite` and `fdatasync` carry a group; `initialise_pwrite` carries the
 * zeroes that reserve capacity. The two are separate because they answer to
 * different contracts: a group's failure is what the durability contract
 * describes, while a failure to reserve capacity is a startup or extension
 * failure that must not consume an injection aimed at a group.
 */
struct WalIo {
  std::function<ssize_t(int, const void*, size_t, off_t)> pwrite;
  std::function<int(int)> fdatasync;
  std::function<ssize_t(int, const void*, size_t, off_t)> initialise_pwrite;
  std::function<ssize_t(int, void*, size_t, off_t)> pread;

  static WalIo Posix();
};

/**
 * The single write-ahead log: one file of epoch frames, written by one flusher.
 *
 * Frames are written in place, at an offset the instance tracks, into a region
 * whose blocks were already allocated and written out with zeroes. Letting the
 * file grow can make a group's fdatasync persist size, allocation, or extent-state
 * metadata as well as data. On filesystems that represent preallocation as
 * unwritten extents, posix_fallocate alone may leave the first overwrite with
 * metadata conversion work. Explicit zero initialisation moves that work before
 * the group flush; the size of the benefit is filesystem- and device-specific.
 *
 * Two consequences run through the rest of this class. The end of the log is not
 * the end of the file: it is where the zeroes begin, which is why nothing may be
 * written before ScanAndRepair has found it. And the zeroes ahead of the log are
 * an invariant, not an accident: they are what makes an interrupted write
 * recognisable, so a repair restores them.
 *
 * Frame layout, little-endian:
 *   magic(u32) version(u16) flags(u16) payload_len(u32) epoch(u32) crc32c(u32)
 *   payload
 *
 * The checksum covers the header up to but excluding the crc field, plus the
 * payload, so a bit flip in the epoch or length is detected too. The payload is
 * the msgpack encoding of every record committed in that epoch. Frame epochs are
 * non-decreasing, and an epoch with no records contributes no frame.
 */
class Wal {
 public:
  /**
   * Opens the log and takes an exclusive lock on it. Nothing is allocated and
   * nothing is read yet: ScanAndRepair does both, in that order, because the
   * region to initialise is the one the scan finds to be past the log.
   *
   * `initial_capacity_bytes` is how much is made writable in place at a time. It
   * is a granularity rather than a limit: a log that outgrows it is extended by
   * the same amount again, at the price of one synchronous initialisation.
   * `kNoPreallocation` leaves the file to grow as it is written, which is what the
   * Volatile contract is given: it writes no record at all, so reserving would
   * occupy the space for nothing.
   */
  Wal(const std::string& work_dir, WalIo io = WalIo::Posix(),
      uint64_t initial_capacity_bytes = kDefaultCapacityBytes);
  ~Wal();

  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;

  /**
   * Reads the log from the beginning, repairs an interrupted tail, initialises
   * the capacity beyond it, and returns the frontier with every decoded record.
   * Must succeed before the first group: it is what locates the end of the log,
   * and until the bytes of an interrupted write are overwritten with zeroes a
   * later shorter group would leave them behind as a frame the next scan cannot
   * place.
   *
   * A frame at or below `min_epoch` is counted but not decoded, for a caller
   * that already holds the state those frames would rebuild; the frontier and
   * the end of the log still come from every frame. Such a frame is hopped over
   * by its header alone, without reading or checksumming its payload, except
   * for the last one before the first frame that is decoded (or the last one in
   * the log, if every frame is at or below `min_epoch`): that one frame is read
   * and its checksum taken, as a cheap guard on the boundary the caller is
   * trusting. A hop that lands on a header which does not parse is not by
   * itself taken as damage, since the length it trusted was never checked; the
   * scan falls back to reading and checksumming every frame from offset 0, the
   * same as passing `min_epoch = 0`, and diagnoses the file from there.
   */
  WalScanResult ScanAndRepair(EpochNumber min_epoch = 0);

  /**
   * Writes one frame per bucket whose epoch is at or below `target`, in epoch
   * order, then issues a single fdatasync. Buckets above `target` are ignored and
   * stay the caller's to carry forward. Returns failure without having advanced
   * anything the caller may publish.
   *
   * A failure with a write or a sync behind it also refuses every later group:
   * where the log ends is no longer known. A payload too large to be framed is
   * refused before any I/O and leaves the instance usable.
   */
  WalAppendResult AppendGroup(const std::map<EpochNumber, LogRecords>& buckets,
                             EpochNumber target);

  const std::string& path() const { return path_; }

  /** Offset one past the last frame, which is where the next group lands. */
  off_t write_offset() const { return write_offset_; }

  /**
   * The epoch of the last frame actually on disk, safe to read from a thread
   * other than the one that owns this instance between StartFlusher and the
   * join.
   *
   * This moves only when a frame is written: an epoch that closed without a
   * record advances the durable epoch a commit waits on, but it advances this
   * not at all, which is what a caller needs from it when the question is
   * what the log itself can be trusted to still hold after a crash.
   */
  EpochNumber frontier() const {
    return frontier_.load(std::memory_order_seq_cst);
  }

  /**
   * How many times capacity had to be extended. Extension is synchronous and
   * writes out a whole new region, so a measurement that means to see the cost of
   * a group flush alone has to report this as zero.
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
   * Outcome of looking for a frame at one offset. A read that fails is its own
   * answer and never a "no": what follows a negative answer is a repair that
   * erases bytes, so an unreadable candidate has to stop the scan instead.
   */
  enum class Probe { NoFrame, Frame, IoError };

  WalScanResult Corrupt(const std::string& detail);
  WalScanResult IoFailure(const std::string& operation, int error);
  WalScanResult FinishScan(WalScanResult&& result, off_t end_of_log);
  bool HopCoveredFrames(EpochNumber min_epoch, off_t file_size, off_t* offset,
                       EpochNumber* frontier, bool* have_frame,
                       size_t* frames_skipped, uint64_t* bytes_skipped,
                       bool* guard_pending, off_t* guard_offset,
                       uint32_t* guard_payload_size, uint8_t* guard_header,
                       int* error) const;
  Probe ProbeFrameAt(off_t offset, off_t file_size, int* error) const;
  Probe SearchForFrameAfter(off_t offset, off_t search_end, off_t file_size,
                            int* error) const;
  bool FindLastNonZero(off_t from, off_t to, off_t* last_non_zero,
                       int* error) const;
  bool EnsureCapacityFor(off_t end_of_log, size_t group_size, int* error);
  bool WriteZeroesAndSync(off_t from, off_t to, int* error);
  bool WriteAllAt(const uint8_t* data, size_t size, off_t offset, int* error);
  bool PreadAll(uint8_t* out, size_t size, off_t offset, int* error) const;

  std::string path_;
  WalIo io_;
  int fd_{-1};
  uint64_t initial_capacity_bytes_;
  State state_{State::Unscanned};
  off_t write_offset_{0};
  /**
   * The file's size, which under preallocation is also the offset below which
   * every block is allocated and holds written-out zeroes.
   */
  off_t initialised_size_{0};
  size_t extension_count_{0};
  std::atomic<EpochNumber> frontier_{0};
};

}  // namespace Recovery
}  // namespace LineairDB

#endif /* LINEAIRDB_RECOVERY_WAL_H */
