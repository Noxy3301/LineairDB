#ifndef LINEAIRDB_RECOVERY_WAL_H
#define LINEAIRDB_RECOVERY_WAL_H

#include <sys/types.h>

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
 * `Ok` means every frame in the file was complete and consistent, and
 * `frontier` is the epoch of the last one; a file with no frames yields
 * frontier 0. An incomplete or checksum-broken frame at the very end of the
 * file is repaired rather than reported: it is physically truncated away,
 * `tail_truncated` is set, and the scan still succeeds. Repair requires that
 * no intact frame follows the damage: a frame past the damage is taken as
 * evidence that the damage sits in a region older appends already synced.
 *
 * `Corrupt` means damage the scan will not repair:
 *   - a checksum-broken complete frame that is not the last bytes of the
 *     file, whatever follows it;
 *   - a damaged tail with an intact frame after the damage, or one the
 *     probe cannot classify within its I/O budget;
 *   - a magic, version or flags anomaly, or a length above the payload cap,
 *     checked before the checksum (a length merely overrunning the file is
 *     a damaged tail and follows the probe rule);
 *   - and, in a complete checksum-valid frame, a regressed or zero epoch or
 *     a payload anomaly.
 * Nothing is truncated and the caller must fail-stop: the file may be
 * missing records that were already acknowledged as durable.
 *
 * @note Ambiguity is resolved toward fail-stop: a torn group whose pages
 * persisted out of order, or torn user data that embeds a byte-exact intact
 * frame, can fail-stop a repairable file, and manual truncation then
 * recovers it. The reverse never happens; repair never discards a frame
 * that another intact frame vouches for, and a tail the probe cannot
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
};

struct WalAppendResult {
  bool ok{true};
  int error_number{0};
};

/**
 * @brief Seam for the two syscalls the append path uses, letting a test
 * inject a write or fdatasync failure.
 * @note Everything else (open, pread, ftruncate, fsync) is always the real
 * syscall.
 * @note LINEAIRDB_WAL_FDATASYNC_FAIL_AFTER=<count> makes the fdatasync that
 * Posix() returns let that many calls through and fail every later call with
 * EIO. Each Posix() call creates one counter, shared by every copy of the
 * WalIo it returned. A value that is not decimal digits, or that does not
 * fit in a long, stops startup.
 */
struct WalIo {
  std::function<ssize_t(int, const void*, size_t)> write;
  std::function<int(int)> fdatasync;

  static WalIo Posix();
};

/**
 * @brief The single write-ahead log: one file of epoch frames with a single
 * appender.
 *
 * @details
 * Frame layout, little-endian:
 *   magic(u32) version(u16) flags(u16) payload_len(u32) epoch(u32) crc32c(u32)
 *   payload
 *
 * The checksum covers the header up to but excluding the crc field, plus the
 * payload, so a bit flip in the epoch or length is detected too. The payload
 * is the msgpack encoding of a non-empty record list carrying the frame's
 * epoch, and frame epochs are non-decreasing. No frame ever carries an empty
 * record list; the caller keeps empty buckets out of AppendGroup, which
 * refuses them rather than skipping them.
 *
 * @note Nothing here locks; a second concurrent appender is undefined.
 */
class Wal {
 public:
  Wal(const std::string& work_dir, WalIo io = WalIo::Posix());
  ~Wal();

  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;

  /**
   * @brief Reads the file from the beginning, repairs an interrupted tail,
   * and returns the frontier with every decoded record.
   * @return See WalScanResult; `Ok` carries the frontier and the records.
   * @note Must succeed before the first append, and AppendGroup refuses to
   * run without it: until the invalid tail is physically removed, an
   * O_APPEND write would land after the broken bytes and turn them into
   * mid-file corruption.
   */
  WalScanResult ScanAndRepair();

  /**
   * @brief Appends one frame per bucket whose epoch is at or below `target`,
   * in epoch order, as one buffered group write followed by one fdatasync
   * (both retried on EINTR, the write also on short counts).
   * @param[in] buckets Records grouped by their commit epoch. Buckets above
   * `target` are ignored and stay the caller's to carry forward.
   * @param[in] target The highest epoch this call may write.
   * @return Failure is returned without having advanced anything the caller
   * may publish. A bucket that would produce a frame the scan rejects
   * (empty, epoch zero, an epoch below the file's frontier, a record epoch
   * disagreeing with its bucket) fails with EINVAL before anything is
   * written, as does any append attempted before a successful scan.
   * @note After a failed write or sync the file may end in a torn frame,
   * and every later call fails without touching the file: appending past a
   * tear would turn it into mid-file corruption.
   */
  WalAppendResult AppendGroup(const std::map<EpochNumber, LogRecords>& buckets,
                             EpochNumber target);

  const std::string& path() const { return path_; }

  static constexpr uint32_t kMagic = 0x4c57414c;  // "LAWL"
  static constexpr uint16_t kVersion = 1;
  static constexpr uint16_t kFlags = 0;
  static constexpr size_t kHeaderSize = 20;
  static constexpr uint32_t kMaxPayloadSize = 256u * 1024u * 1024u;

 private:
  enum class TailProbe { NoIntactFrame, IntactFrame, Undecidable, IoError };

  WalScanResult Corrupt(const std::string& detail) const;
  WalScanResult IoFailure(const std::string& operation, int error) const;
  WalScanResult TruncateTail(off_t last_good, EpochNumber frontier,
                             LogRecords&& records);
  TailProbe ProbeTailForAnIntactFrame(off_t damage, off_t file_size,
                                      int* error) const;
  bool WriteAll(const uint8_t* data, size_t size, int* error);
  bool PreadAll(uint8_t* out, size_t size, off_t offset, int* error) const;

  std::string path_;
  WalIo io_;
  int fd_{-1};
  bool scanned_{false};
  bool repair_unsynced_{false};
  EpochNumber frontier_{0};
  bool append_failed_{false};
};

}  // namespace Recovery
}  // namespace LineairDB

#endif /* LINEAIRDB_RECOVERY_WAL_H */
