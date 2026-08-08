#include <gtest/gtest.h>
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "lineairdb/config.h"
#include "lineairdb/database.h"
#include "lineairdb/stateless.h"
#include "recovery/epoch_scan_checkpoint.h"
#include "recovery/wal.h"

namespace {

constexpr const char* kTable = "__anonymous_table";
constexpr const char* kIndex = "idx";
constexpr auto kTestTimeout = std::chrono::seconds(10);

using LineairDB::Recovery::EpochScanCheckpoint;
using LineairDB::Recovery::Wal;
using LineairDB::Recovery::WalScanResult;

// Closes both ends on scope exit so an assertion failure cannot leak them.
// Ported from debug_sync_test.cpp.
class Pipe {
 public:
  Pipe() { EXPECT_EQ(::pipe(fds_), 0); }
  ~Pipe() {
    CloseRead();
    CloseWrite();
  }
  int read_fd() const { return fds_[0]; }
  int write_fd() const { return fds_[1]; }
  void CloseRead() { Close(&fds_[0]); }
  void CloseWrite() { Close(&fds_[1]); }

 private:
  static void Close(int* fd) {
    if (*fd >= 0) {
      ::close(*fd);
      *fd = -1;
    }
  }
  int fds_[2] = {-1, -1};
};

// Writes the release byte on scope exit. Declared after the future so it runs
// first and the blocked point can always finish; the extra byte on the
// success path is never read and harmless. Ported from debug_sync_test.cpp.
struct ReleaseOnExit {
  int fd;
  ~ReleaseOnExit() {
    [[maybe_unused]] const ssize_t rc = ::write(fd, "r", 1);
  }
};

// Closes the write end on scope exit. Declared after the future whose read
// loop owns the other end, so an early return delivers the EOF that ends it
// before that future's destructor would otherwise block joining it.
struct CloseWriteOnExit {
  Pipe& pipe;
  ~CloseWriteOnExit() { pipe.CloseWrite(); }
};

// True once `fd` has data to read, false if `timeout` passes first. Bounds an
// arrival wait that would otherwise block forever if the point never fires.
bool WaitReadable(int fd, std::chrono::milliseconds timeout) {
  pollfd target{fd, POLLIN, 0};
  const int rc = ::poll(&target, 1, static_cast<int>(timeout.count()));
  return rc == 1 && (target.revents & POLLIN) != 0;
}

class EpochScanCheckpointTest : public ::testing::Test {
 protected:
  // The sync facility decides once per process whether anything is armed, so
  // one variable stays set for every test in this binary.
  static void SetUpTestSuite() {
    ::setenv("LINEAIRDB_DEBUG_SYNC_KEEPS_THE_FACILITY_ARMED", "sleep:0", 1);
  }

  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "lineairdb_ckpt_XXXXXX")
            .string();
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    ASSERT_NE(::mkdtemp(buffer.data()), nullptr);
    root_ = buffer.data();
    work_dir_ = root_ + "/logs";
  }

  void TearDown() override {
    for (const auto& variable : armed_) ::unsetenv(variable.c_str());
    armed_.clear();
    std::error_code ec;
    std::filesystem::remove_all(root_, ec);
  }

  void Arm(const std::string& variable, const std::string& action) {
    ::setenv(variable.c_str(), action.c_str(), 1);
    armed_.push_back(variable);
  }

  LineairDB::Config MakeConfig(bool enable_recovery) const {
    LineairDB::Config config;
    config.max_thread = 1;
    config.epoch_duration_ms = 10;
    config.concurrency_control_protocol =
        LineairDB::Config::ConcurrencyControl::Silo;
    config.index_structure = LineairDB::Config::IndexStructure::Masstree;
    config.commit_durability = LineairDB::Config::CommitDurability::Sync;
    config.enable_checkpointing = false;
    config.enable_recovery = enable_recovery;
    config.work_dir = work_dir_;
    config.wal_initial_capacity_bytes = 1ull << 20;
    return config;
  }

  static bool CommitWrite(LineairDB::Database& db, const std::string& key,
                          const std::string& value) {
    const bool committed =
        db.ValidateAndCommit({}, {{kTable, key, value, false}}, {}, {});
    db.ReleaseMasstreeThreadEpoch();
    return committed;
  }

  static bool CommitDelete(LineairDB::Database& db, const std::string& key) {
    const bool committed =
        db.ValidateAndCommit({}, {{kTable, key, "", true}}, {}, {});
    db.ReleaseMasstreeThreadEpoch();
    return committed;
  }

  static bool CommitIndexedWrite(LineairDB::Database& db,
                                 const std::string& key,
                                 const std::string& value,
                                 const std::string& secondary_key) {
    const bool committed = db.ValidateAndCommit(
        {}, {{kTable, key, value, false}},
        {{kTable, kIndex, secondary_key, key, false}}, {});
    db.ReleaseMasstreeThreadEpoch();
    return committed;
  }

  static LineairDB::StatelessReadResult Read(LineairDB::Database& db,
                                             const std::string& key) {
    auto result = db.StatelessRead(kTable, key);
    db.ReleaseMasstreeThreadEpoch();
    return result;
  }

  /** Every key's value, in key order, as the database currently holds it. */
  static std::vector<std::string> ReadAll(LineairDB::Database& db) {
    std::vector<std::string> rows;
    for (const char* key : {"alice", "bob", "carol"}) {
      const auto row = Read(db, key);
      rows.emplace_back(std::string(key) + "=" + (row.found ? row.value : ""));
    }
    return rows;
  }

  /** Every secondary-index hit, as `secondary_key/primary_key=value`. */
  static std::vector<std::string> ReadIndex(LineairDB::Database& db) {
    auto result = db.StatelessSecondaryRangeScan(kTable, kIndex, "", "\xff", 0,
                                                 false);
    db.ReleaseMasstreeThreadEpoch();
    std::vector<std::string> hits;
    for (const auto& row : result.rows) {
      hits.emplace_back(row.secondary_key + "/" + row.primary_key + "=" +
                        row.value);
    }
    std::sort(hits.begin(), hits.end());
    return hits;
  }

  /** The row value the image holds for `key`, if it holds one. */
  static std::optional<std::string> RowInImage(
      const EpochScanCheckpoint::Image& image, const std::string& key) {
    for (const auto& record : image.records) {
      for (const auto& kvp : record.key_value_pairs) {
        if (!kvp.index_name.empty() || kvp.key != key) continue;
        return kvp.buffer;
      }
    }
    return std::nullopt;
  }

  /** The primary keys the image lists under a secondary key. */
  static std::vector<std::string> IndexEntryInImage(
      const EpochScanCheckpoint::Image& image, const std::string& key) {
    for (const auto& record : image.records) {
      for (const auto& kvp : record.key_value_pairs) {
        if (kvp.index_name != kIndex || kvp.key != key) continue;
        return kvp.primary_keys;
      }
    }
    return {};
  }

  std::string image_path() const {
    return (std::filesystem::path(work_dir_) /
            EpochScanCheckpoint::ImageFileName())
        .string();
  }

  std::string working_path() const {
    return (std::filesystem::path(work_dir_) /
            EpochScanCheckpoint::WorkingFileName())
        .string();
  }

  std::string root_;
  std::string work_dir_;
  LineairDB::EpochNumber frontier_ = 0;

 private:
  std::vector<std::string> armed_;
};

TEST_F(EpochScanCheckpointTest, AnImageHoldsWhatTheScanFound) {
  {
    auto config = MakeConfig(false);
    LineairDB::Database db(config);
    ASSERT_TRUE(db.CreateSecondaryIndex(kTable, kIndex, 0));
    ASSERT_TRUE(CommitIndexedWrite(db, "alice", "one", "s"));
    ASSERT_TRUE(CommitIndexedWrite(db, "bob", "two", "s"));
    ASSERT_TRUE(db.WriteCheckpointImage());
  }

  auto image = EpochScanCheckpoint::Load(work_dir_);
  ASSERT_EQ(image.status, EpochScanCheckpoint::Image::Status::Ok);
  EXPECT_EQ(RowInImage(image, "alice"), "one");
  EXPECT_EQ(RowInImage(image, "bob"), "two");
  auto primary_keys = IndexEntryInImage(image, "s");
  std::sort(primary_keys.begin(), primary_keys.end());
  EXPECT_EQ(primary_keys, (std::vector<std::string>{"alice", "bob"}));
  EXPECT_NE(image.cut_epoch, 0u);
  EXPECT_GE(image.end_epoch, image.cut_epoch);
  // The working file is renamed rather than left behind.
  EXPECT_FALSE(std::filesystem::exists(working_path()));
}

TEST_F(EpochScanCheckpointTest, ADeletedRowLeavesNoEntry) {
  {
    auto config = MakeConfig(false);
    LineairDB::Database db(config);
    ASSERT_TRUE(CommitWrite(db, "alice", "one"));
    ASSERT_TRUE(CommitWrite(db, "bob", "two"));
    ASSERT_TRUE(CommitDelete(db, "alice"));
    ASSERT_TRUE(db.WriteCheckpointImage());
  }

  auto image = EpochScanCheckpoint::Load(work_dir_);
  ASSERT_EQ(image.status, EpochScanCheckpoint::Image::Status::Ok);
  EXPECT_EQ(RowInImage(image, "alice"), std::nullopt);
  EXPECT_EQ(RowInImage(image, "bob"), "two");
}

TEST_F(EpochScanCheckpointTest, AnAbsentImageIsNotAFailure) {
  auto image = EpochScanCheckpoint::Load(work_dir_ + "/nowhere");
  EXPECT_EQ(image.status, EpochScanCheckpoint::Image::Status::Absent);
}

TEST_F(EpochScanCheckpointTest, ADamagedImageIsRefused) {
  {
    auto config = MakeConfig(false);
    LineairDB::Database db(config);
    ASSERT_TRUE(CommitWrite(db, "alice", "one"));
    ASSERT_TRUE(db.WriteCheckpointImage());
  }

  // One byte inside the payload, which the checksum covers.
  {
    std::fstream file(image_path(),
                      std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.is_open());
    file.seekp(static_cast<std::streamoff>(EpochScanCheckpoint::kHeaderSize));
    char flipped = 0x7f;
    file.write(&flipped, 1);
  }

  auto image = EpochScanCheckpoint::Load(work_dir_);
  EXPECT_EQ(image.status, EpochScanCheckpoint::Image::Status::Unusable);
  EXPECT_TRUE(image.records.empty());
}

TEST_F(EpochScanCheckpointTest, TheLogTailWinsOverTheImage) {
  {
    auto config = MakeConfig(false);
    LineairDB::Database db(config);
    ASSERT_TRUE(CommitWrite(db, "alice", "one"));
    ASSERT_TRUE(CommitWrite(db, "bob", "one"));
    ASSERT_TRUE(CommitWrite(db, "carol", "one"));
    ASSERT_TRUE(db.WriteCheckpointImage());
    // Written after the cut: the image holds the old version of alice and no
    // version of dave, and the tail has to supply both.
    ASSERT_TRUE(CommitWrite(db, "alice", "two"));
    ASSERT_TRUE(CommitDelete(db, "carol"));
    ASSERT_TRUE(CommitWrite(db, "dave", "two"));
  }

  auto config = MakeConfig(true);
  LineairDB::Database db(config);
  EXPECT_EQ(Read(db, "alice").value, "two");
  // Only the image holds this one: its record is in a frame the replay skips.
  EXPECT_EQ(Read(db, "bob").value, "one");
  EXPECT_FALSE(Read(db, "carol").found);
  EXPECT_EQ(Read(db, "dave").value, "two");
}

TEST_F(EpochScanCheckpointTest, RecoveryWithTheImageMatchesRecoveryWithout) {
  {
    auto config = MakeConfig(false);
    LineairDB::Database db(config);
    ASSERT_TRUE(db.CreateSecondaryIndex(kTable, kIndex, 0));
    ASSERT_TRUE(CommitIndexedWrite(db, "alice", "one", "s"));
    ASSERT_TRUE(CommitIndexedWrite(db, "bob", "one", "t"));
    ASSERT_TRUE(db.WriteCheckpointImage());
    ASSERT_TRUE(CommitWrite(db, "alice", "two"));
    ASSERT_TRUE(CommitDelete(db, "bob"));
    ASSERT_TRUE(CommitIndexedWrite(db, "carol", "two", "s"));
  }

  const auto image = EpochScanCheckpoint::Load(work_dir_);
  ASSERT_EQ(image.status, EpochScanCheckpoint::Image::Status::Ok);

  // What the replay leaves out, and that it leaves out something at all.
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), 1ull << 20);
    auto full = wal.ScanAndRepair(0);
    ASSERT_EQ(full.status, WalScanResult::Status::Ok);
    EXPECT_EQ(full.frames_skipped, 0u);
    frontier_ = full.frontier;
  }
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), 1ull << 20);
    auto filtered = wal.ScanAndRepair(image.cut_epoch);
    ASSERT_EQ(filtered.status, WalScanResult::Status::Ok);
    EXPECT_GT(filtered.frames_skipped, 0u);
    EXPECT_GT(filtered.bytes_skipped, 0u);
    // The end of the log and how far it is durable come from every frame.
    EXPECT_EQ(filtered.frontier, frontier_);
    for (const auto& record : filtered.records) {
      EXPECT_GT(record.epoch, image.cut_epoch);
    }
  }

  std::vector<std::string> with_image;
  std::vector<std::string> index_with_image;
  {
    auto config = MakeConfig(true);
    LineairDB::Database db(config);
    with_image = ReadAll(db);
    index_with_image = ReadIndex(db);
  }

  std::error_code ec;
  ASSERT_TRUE(std::filesystem::remove(image_path(), ec)) << ec.message();
  std::vector<std::string> without_image;
  std::vector<std::string> index_without_image;
  {
    auto config = MakeConfig(true);
    LineairDB::Database db(config);
    without_image = ReadAll(db);
    index_without_image = ReadIndex(db);
  }

  EXPECT_EQ(with_image, without_image);
  EXPECT_EQ(with_image,
            (std::vector<std::string>{"alice=two", "bob=", "carol=two"}));
  EXPECT_EQ(index_with_image, index_without_image);
  // The index reaches the row the tail rewrote and the one it added, and no
  // longer reaches the row the tail deleted.
  EXPECT_EQ(index_with_image,
            (std::vector<std::string>{"s/alice=two", "s/carol=two"}));
}

TEST_F(EpochScanCheckpointTest, AQuietTailAfterTheImageIsAccepted) {
  {
    auto config = MakeConfig(false);
    LineairDB::Database db(config);
    ASSERT_TRUE(CommitWrite(db, "alice", "one"));
    ASSERT_TRUE(CommitWrite(db, "bob", "one"));
    // Nothing is written afterwards, so the scan ends past the epoch of the
    // last frame the log holds: the database went quiet before the image did.
    ASSERT_TRUE(db.WriteCheckpointImage());
  }

  const auto image = EpochScanCheckpoint::Load(work_dir_);
  ASSERT_EQ(image.status, EpochScanCheckpoint::Image::Status::Ok);
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), 1ull << 20);
    auto scan = wal.ScanAndRepair(0);
    ASSERT_EQ(scan.status, WalScanResult::Status::Ok);
    // The quiet tail this test is named for: the log's frontier never
    // reaches the epoch the scan ended at, which is what made the v1 gate
    // refuse a legitimate image.
    ASSERT_LT(scan.frontier, image.end_epoch);
    // The v2 gate asks a question this log still answers: it reaches at
    // least as far as the log was durable when the image was published.
    ASSERT_GE(scan.frontier, image.wal_frontier_at_publish);
  }

  auto config = MakeConfig(true);
  LineairDB::Database db(config);
  EXPECT_EQ(Read(db, "alice").value, "one");
  EXPECT_EQ(Read(db, "bob").value, "one");
}

TEST_F(EpochScanCheckpointTest, ALogShorterThanThePublishFrontierIsRejected) {
  const std::string short_log_copy = root_ + "/short_wal.log";
  {
    auto config = MakeConfig(false);
    LineairDB::Database db(config);
    ASSERT_TRUE(CommitWrite(db, "alice", "one"));
    ASSERT_TRUE(CommitWrite(db, "bob", "one"));
    // A copy of the log as it stands here, before the commits the image
    // published below will require the log to reach.
    std::filesystem::copy_file(work_dir_ + "/wal.log", short_log_copy);
    ASSERT_TRUE(CommitWrite(db, "carol", "one"));
    ASSERT_TRUE(CommitWrite(db, "dave", "one"));
    ASSERT_TRUE(db.WriteCheckpointImage());
  }

  const auto image = EpochScanCheckpoint::Load(work_dir_);
  ASSERT_EQ(image.status, EpochScanCheckpoint::Image::Status::Ok);

  // Stand in for a log genuinely truncated, or substituted, after the image
  // was published: put the earlier, shorter log back in its place.
  std::filesystem::copy_file(
      short_log_copy, work_dir_ + "/wal.log",
      std::filesystem::copy_options::overwrite_existing);
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), 1ull << 20);
    const auto scan = wal.ScanAndRepair(0);
    ASSERT_EQ(scan.status, WalScanResult::Status::Ok);
    ASSERT_LT(scan.frontier, image.wal_frontier_at_publish);
  }

  // The refusal is only real if recovery acts on it: rows the image alone
  // holds must not come back from a log that never carried them.
  auto config = MakeConfig(true);
  LineairDB::Database db(config);
  EXPECT_EQ(Read(db, "alice").value, "one");
  EXPECT_EQ(Read(db, "bob").value, "one");
  EXPECT_FALSE(Read(db, "carol").found);
  EXPECT_FALSE(Read(db, "dave").found);
}

TEST_F(EpochScanCheckpointTest, AV1FormatImageIsRefused) {
  {
    auto config = MakeConfig(false);
    LineairDB::Database db(config);
    ASSERT_TRUE(CommitWrite(db, "alice", "one"));
    ASSERT_TRUE(db.WriteCheckpointImage());
  }

  // Downgrade the version field to what a v1 writer would have left. There is
  // no migration for it: v1 has no wal_frontier_at_publish field to read.
  {
    std::fstream file(image_path(),
                      std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.is_open());
    file.seekp(4);
    const uint8_t v1_version[2] = {0x01, 0x00};
    file.write(reinterpret_cast<const char*>(v1_version), sizeof(v1_version));
  }

  auto image = EpochScanCheckpoint::Load(work_dir_);
  EXPECT_EQ(image.status, EpochScanCheckpoint::Image::Status::Unusable);
  EXPECT_TRUE(image.records.empty());
}

TEST_F(EpochScanCheckpointTest, ARowLockedDuringTheScanIsRetried) {
  Pipe scan_arrived;
  Pipe scan_release;
  Pipe write_arrived;
  Pipe write_release;

  auto config = MakeConfig(false);
  LineairDB::Database db(config);
  ASSERT_TRUE(CommitWrite(db, "alice", std::string(64, 'a')));
  ASSERT_TRUE(CommitWrite(db, "bob", std::string(64, 'a')));

  Arm("LINEAIRDB_DEBUG_SYNC_CHECKPOINT_BEFORE_ROW_COPY",
      "arrive_and_wait:" + std::to_string(scan_arrived.write_fd()) + ":" +
          std::to_string(scan_release.read_fd()));
  Arm("LINEAIRDB_DEBUG_SYNC_STATELESS_COMMIT_BETWEEN_ROW_INSTALLS",
      "arrive_and_wait:" + std::to_string(write_arrived.write_fd()) + ":" +
          std::to_string(write_release.read_fd()));

  uint64_t version_retries = 0;
  auto scan = std::async(std::launch::async, [&db, &version_retries] {
    return db.WriteCheckpointImage(&version_retries);
  });
  // See ReleaseOnExit: unblocks a scan parked at its current wait before
  // `scan`'s own destructor would otherwise join it forever.
  ReleaseOnExit release_scan_on_exit{scan_release.write_fd()};
  // Reverse destruction must close before the release byte fires below, or a
  // scan released back into an open pipe just re-arrives and parks again;
  // this guard covers the window before the releaser exists.
  CloseWriteOnExit close_scan_arrived_early{scan_arrived};

  // The scan has loaded a version and is about to copy the bytes it belongs
  // to; nothing has locked that row yet.
  char announcement = 0;
  ASSERT_TRUE(WaitReadable(scan_arrived.read_fd(), kTestTimeout));
  ASSERT_EQ(::read(scan_arrived.read_fd(), &announcement, 1), 1);

  auto writer = std::async(std::launch::async, [&db] {
    const bool committed = db.ValidateAndCommit(
        {},
        {{kTable, "alice", std::string(64, 'b'), false},
         {kTable, "bob", std::string(64, 'b'), false}},
        {}, {});
    db.ReleaseMasstreeThreadEpoch();
    return committed;
  });
  ReleaseOnExit release_write_on_exit{write_release.write_fd()};

  // The writer holds both rows: one is installed, the other is not.
  ASSERT_TRUE(WaitReadable(write_arrived.read_fd(), kTestTimeout));
  ASSERT_EQ(::read(write_arrived.read_fd(), &announcement, 1), 1);
  // Releasing the scan here makes it copy bytes the writer is changing, which
  // its second version read has to reject.
  ASSERT_EQ(::write(scan_release.write_fd(), "r", 1), 1);

  // From here the scan may reach the point again on any retry, so arrivals are
  // answered by a thread of their own.
  auto releaser = std::async(std::launch::async, [&] {
    for (;;) {
      char arrived = 0;
      if (::read(scan_arrived.read_fd(), &arrived, 1) != 1) return;
      if (::write(scan_release.write_fd(), "r", 1) != 1) return;
    }
  });
  // See CloseWriteOnExit: an early return still delivers the EOF the
  // releaser's read loop above is waiting on.
  CloseWriteOnExit close_scan_arrived_on_exit{scan_arrived};

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ASSERT_EQ(::write(write_release.write_fd(), "r", 1), 1);
  ASSERT_EQ(writer.wait_for(kTestTimeout), std::future_status::ready);
  EXPECT_TRUE(writer.get());

  ASSERT_EQ(scan.wait_for(kTestTimeout), std::future_status::ready);
  EXPECT_TRUE(scan.get());
  // The writer held both rows for the whole of its park, so a scan that
  // reported no retry did not read them while they were held.
  EXPECT_GT(version_retries, 0u);
  scan_arrived.CloseWrite();
  ASSERT_EQ(releaser.wait_for(kTestTimeout), std::future_status::ready);
  releaser.get();

  auto image = EpochScanCheckpoint::Load(work_dir_);
  ASSERT_EQ(image.status, EpochScanCheckpoint::Image::Status::Ok);
  // Either version is a correct answer for a scan that runs alongside a
  // writer. A mixture of the two is not.
  for (const char* key : {"alice", "bob"}) {
    const auto value = RowInImage(image, key);
    ASSERT_TRUE(value.has_value()) << key;
    EXPECT_TRUE(*value == std::string(64, 'a') ||
                *value == std::string(64, 'b'))
        << key << " holds " << *value;
  }
}

TEST_F(EpochScanCheckpointTest, ALeftoverWorkingFileIsNotRead) {
  {
    auto config = MakeConfig(false);
    LineairDB::Database db(config);
    ASSERT_TRUE(CommitWrite(db, "alice", "one"));
    ASSERT_TRUE(db.WriteCheckpointImage());
  }

  // What a crash between the write and the rename leaves behind.
  {
    std::ofstream file(working_path(), std::ios::binary);
    ASSERT_TRUE(file.is_open());
    file << "half of an image";
  }

  auto image = EpochScanCheckpoint::Load(work_dir_);
  ASSERT_EQ(image.status, EpochScanCheckpoint::Image::Status::Ok);
  EXPECT_EQ(RowInImage(image, "alice"), "one");
}

}  // namespace
