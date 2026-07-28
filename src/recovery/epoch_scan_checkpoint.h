#ifndef LINEAIRDB_RECOVERY_EPOCH_SCAN_CHECKPOINT_H
#define LINEAIRDB_RECOVERY_EPOCH_SCAN_CHECKPOINT_H

#include <lineairdb/config.h>

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include "log_record.h"
#include "types/definitions.h"

namespace LineairDB {

struct DataItem;
class Table;
class TableDictionary;
class EpochFramework;

namespace Recovery {

class Logger;

/**
 * An image of the live rows, written while transactions keep running.
 *
 * The scan begins after EpochFramework::Sync, which is what makes the image
 * usable: once it returns, no thread is still online in an epoch at or below
 * the one recorded as the cut, so every commit at or below the cut has landed
 * in memory. Rows committed during the scan may be captured as well, and the
 * image is fuzzy in exactly that sense. Recovery folds the image under the
 * same newest-transaction-id-wins rule as the log and replays only the frames
 * above the cut, which resolves the mixture: a row the scan captured early is
 * overwritten by a later record, and a row it captured late already agrees
 * with one.
 *
 * A row is copied under the version protocol the read path uses, so the image
 * never holds a value torn by a concurrent install. The image is published
 * only once the log covers every epoch the scan could have observed, so a
 * recovery that reads it never installs a transaction whose record was lost.
 *
 * Nothing here bounds the size of the log on disk. What the image bounds is
 * how much of it recovery has to replay.
 */
class EpochScanCheckpoint {
 public:
  /** What one capture did, for the line it logs when it completes. */
  struct Stats {
    uint64_t generation{0};
    EpochNumber cut_epoch{0};
    EpochNumber end_epoch{0};
    // The epoch of the log's last frame once the wait for `end_epoch` to be
    // durable has returned, read from Wal::frontier rather than from the
    // durable epoch a commit waits on: that epoch can reach `end_epoch`
    // through a run of closed-but-empty epochs alone, which is a promise
    // about what the log does not need, not about a frame it has. This one
    // moves only when a frame is written, so it is what recovery's
    // acceptance gate compares the rescanned log against.
    EpochNumber wal_frontier_at_publish{0};
    uint64_t primary_rows{0};
    uint64_t secondary_entries{0};
    uint64_t image_bytes{0};
    uint64_t retries{0};
    int64_t barrier_ms{0};
    int64_t scan_ms{0};
    int64_t write_ms{0};
    int64_t gate_ms{0};
  };

  /**
   * A published image as recovery receives it.
   *
   * `Absent` and `Unusable` are both answered with a full replay of the log;
   * they are distinguished so that a damaged image is reported rather than
   * passed over in silence.
   */
  struct Image {
    enum class Status { Ok, Absent, Unusable };

    Status status{Status::Absent};
    EpochNumber cut_epoch{0};
    EpochNumber end_epoch{0};
    EpochNumber wal_frontier_at_publish{0};
    LogRecords records;
    std::string detail;
  };

  EpochScanCheckpoint(const Config& config, TableDictionary& tables,
                      EpochFramework& epoch_framework, Logger& logger);
  ~EpochScanCheckpoint();

  EpochScanCheckpoint(const EpochScanCheckpoint&) = delete;
  EpochScanCheckpoint& operator=(const EpochScanCheckpoint&) = delete;

  /**
   * Starts the thread that captures on the configured interval. A zero
   * interval and no one-shot delay leaves the thread unstarted, which is the
   * default. Must follow EpochFramework::Start.
   */
  void Start();

  /** Stops the thread, waiting for a capture in progress to finish. */
  void Stop();

  /**
   * Captures one image and publishes it, or abandons the attempt and leaves
   * the previously published image in place. Returns whether it published.
   */
  bool RunOnce(Stats* out_stats = nullptr);

  /** Reads the published image of `work_dir`, if there is a usable one. */
  static Image Load(const std::string& work_dir);

  /** Name of the published image inside the working directory. */
  static const char* ImageFileName();
  /** Name of the file a capture writes before it publishes. */
  static const char* WorkingFileName();

  static constexpr uint32_t kMagic = 0x504b434c;  // "LCKP"
  // v2 adds wal_frontier_at_publish; a v1 image has no such field and is
  // refused rather than read as one, since there is no value to fall back to
  // that would not misstate what the log was asked to reach.
  static constexpr uint16_t kVersion = 2;
  static constexpr uint16_t kFlags = 0;
  static constexpr size_t kHeaderSize = 56;

 private:
  /** What one attempt at one row produced. */
  enum class Capture { Taken, Skipped, Unstable };

  /** Whether this configuration can produce a usable image. */
  bool Supported() const;

  static Capture CapturePrimaryRow(const std::string& table_name,
                                   std::string_view key, const DataItem& item,
                                   LogRecord::KeyValuePair* out,
                                   uint64_t* retries);
  static Capture CaptureSecondaryEntry(const std::string& table_name,
                                       const std::string& index_name,
                                       uint32_t index_type,
                                       std::string_view key,
                                       const DataItem& item,
                                       LogRecord::KeyValuePair* out,
                                       uint64_t* retries);
  bool CaptureTable(Table& table, LogRecord* record, Stats* stats);
  bool Publish(const LogRecords& records, Stats* stats);
  void Loop();
  bool WaitFor(uint64_t milliseconds);

  const Config& config_;
  TableDictionary& tables_;
  EpochFramework& epoch_framework_;
  Logger& logger_;
  const std::string image_path_;
  const std::string working_path_;

  uint64_t generation_{0};
  // One capture at a time: two would share the working file, and the older
  // one's durability gate would publish the newer one's bytes.
  std::mutex capture_mutex_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool stop_{false};
  std::thread thread_;
};

}  // namespace Recovery
}  // namespace LineairDB

#endif /* LINEAIRDB_RECOVERY_EPOCH_SCAN_CHECKPOINT_H */
