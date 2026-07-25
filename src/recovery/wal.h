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
 * Result of scanning the WAL at startup.
 *
 * `Ok` means every frame in the file was complete and consistent, and
 * `frontier` is the epoch of the last one; a file with no frames yields
 * frontier 0. An incomplete or checksum-broken frame at the very end of the
 * file is repaired rather than reported: it is physically truncated away,
 * `tail_truncated` is set, and the scan still succeeds.
 *
 * `Corrupt` means damage that cannot be attributed to an interrupted append
 * (a broken frame with valid frames after it, a non-monotonic epoch, a length
 * or magic anomaly). Nothing is truncated and the caller must fail-stop: the
 * file may be missing records that were already acknowledged as durable.
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
 * Seam for the two syscalls the append path uses, so a test can inject a write
 * or fdatasync failure. Everything else (open, pread, ftruncate, fsync) is
 * always the real syscall.
 */
struct WalIo {
  std::function<ssize_t(int, const void*, size_t)> write;
  std::function<int(int)> fdatasync;

  static WalIo Posix();
};

/**
 * The single write-ahead log: one file of epoch frames, appended by one
 * flusher.
 *
 * Frame layout, little-endian:
 *   magic(u32) version(u16) flags(u16) payload_len(u32) epoch(u32) crc32c(u32)
 *   payload
 *
 * The checksum covers the header up to but excluding the crc field, plus the
 * payload, so a bit flip in the epoch or length is detected too. The payload is
 * the msgpack encoding of every record committed in that epoch. Frame epochs
 * are non-decreasing, and an epoch with no records contributes no frame.
 */
class Wal {
 public:
  Wal(const std::string& work_dir, WalIo io = WalIo::Posix());
  ~Wal();

  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;

  /**
   * Reads the file from the beginning, repairs an interrupted tail, and returns
   * the frontier with every decoded record. Must run before the first append
   * and before the instance accepts work: until the invalid tail is physically
   * removed, an O_APPEND write would land after the broken bytes and turn them
   * into mid-file corruption.
   */
  WalScanResult ScanAndRepair();

  /**
   * Appends one frame per bucket whose epoch is at or below `target`, in epoch
   * order, then issues a single fdatasync. Buckets above `target` are ignored
   * and stay the caller's to carry forward. Returns failure without having
   * advanced anything the caller may publish.
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
  WalScanResult Corrupt(const std::string& detail) const;
  WalScanResult IoFailure(const std::string& operation, int error) const;
  WalScanResult TruncateTail(off_t last_good, EpochNumber frontier,
                             LogRecords&& records);
  bool WriteAll(const uint8_t* data, size_t size, int* error);
  bool PreadAll(uint8_t* out, size_t size, off_t offset, int* error) const;

  std::string path_;
  WalIo io_;
  int fd_{-1};
};

}  // namespace Recovery
}  // namespace LineairDB

#endif /* LINEAIRDB_RECOVERY_WAL_H */
