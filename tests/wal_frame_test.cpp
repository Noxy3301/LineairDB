#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <map>
#include <string>

#include "recovery/crc32c.h"
#include "recovery/wal.h"

namespace {

using LineairDB::EpochNumber;
using LineairDB::Recovery::ComputeCrc32c;
using LineairDB::Recovery::LogRecord;
using LineairDB::Recovery::LogRecords;
using LineairDB::Recovery::Wal;
using LineairDB::Recovery::WalScanResult;

LogRecords MakeRecords(EpochNumber epoch, const std::string& key) {
  LogRecord record;
  record.epoch = epoch;
  LogRecord::KeyValuePair kvp;
  kvp.key = key;
  kvp.buffer = "value-of-" + key;
  kvp.table_name = "t";
  record.key_value_pairs.emplace_back(std::move(kvp));
  return LogRecords{std::move(record)};
}

class WalFrameTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "lineairdb_wal_XXXXXX")
            .string();
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    ASSERT_NE(::mkdtemp(buffer.data()), nullptr);
    root_ = buffer.data();
    work_dir_ = root_ + "/logs";
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(root_, ec);
  }

  std::string wal_path() const { return work_dir_ + "/wal.log"; }

  off_t FileSize() const {
    struct stat file_stat{};
    EXPECT_EQ(::stat(wal_path().c_str(), &file_stat), 0);
    return file_stat.st_size;
  }

  // Flips a bit at an absolute offset, standing in for a torn or damaged write.
  void FlipByteAt(off_t offset) {
    const int fd = ::open(wal_path().c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    uint8_t byte = 0;
    ASSERT_EQ(::pread(fd, &byte, 1, offset), 1);
    byte = static_cast<uint8_t>(byte ^ 0xffu);
    ASSERT_EQ(::pwrite(fd, &byte, 1, offset), 1);
    ASSERT_EQ(::close(fd), 0);
  }

  void AppendEpochs(const std::vector<EpochNumber>& epochs) {
    Wal wal(work_dir_);
    std::map<EpochNumber, LogRecords> buckets;
    for (const auto epoch : epochs) {
      buckets[epoch] = MakeRecords(epoch, "k" + std::to_string(epoch));
    }
    const auto result = wal.AppendGroup(buckets, epochs.back());
    ASSERT_TRUE(result.ok) << "errno " << result.error_number;
  }

  std::string root_;
  std::string work_dir_;
};

TEST_F(WalFrameTest, Crc32cKnownVector) {
  EXPECT_EQ(ComputeCrc32c("123456789", 9), 0xE3069283u);
}

TEST_F(WalFrameTest, ScanOfAFreshLogHasNoFrontier) {
  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Ok);
  EXPECT_EQ(result.frontier, 0u);
  EXPECT_TRUE(result.records.empty());
  EXPECT_FALSE(result.tail_truncated);
}

TEST_F(WalFrameTest, ScanReturnsTheLastCompleteEpoch) {
  AppendEpochs({1, 3});

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok);
  EXPECT_EQ(result.frontier, 3u);
  EXPECT_FALSE(result.tail_truncated);
  ASSERT_EQ(result.records.size(), 2u);
  EXPECT_EQ(result.records[0].epoch, 1u);
  EXPECT_EQ(result.records[1].epoch, 3u);
  EXPECT_EQ(result.records[0].key_value_pairs.at(0).key, "k1");
}

TEST_F(WalFrameTest, GroupSkipsBucketsAboveTheTarget) {
  Wal wal(work_dir_);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  buckets[2] = MakeRecords(2, "k2");
  buckets[3] = MakeRecords(3, "k3");
  ASSERT_TRUE(wal.AppendGroup(buckets, 2).ok);

  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok);
  EXPECT_EQ(result.frontier, 2u);
  EXPECT_EQ(result.records.size(), 2u);
}

TEST_F(WalFrameTest, TailCorruptionIsTruncatedAndLaterAppendsRecover) {
  AppendEpochs({1, 2, 3});
  const off_t full_size = FileSize();

  // Damage the checksum of the last frame only.
  FlipByteAt(full_size - 1);

  off_t repaired_size = 0;
  {
    Wal wal(work_dir_);
    const auto result = wal.ScanAndRepair();
    ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
    EXPECT_TRUE(result.tail_truncated);
    EXPECT_EQ(result.frontier, 2u);
    ASSERT_EQ(result.records.size(), 2u);
    repaired_size = FileSize();
    EXPECT_LT(repaired_size, full_size);
  }

  // The repaired file must accept further appends and read back cleanly.
  {
    Wal wal(work_dir_);
    std::map<EpochNumber, LogRecords> buckets;
    buckets[4] = MakeRecords(4, "k4");
    ASSERT_TRUE(wal.AppendGroup(buckets, 4).ok);
    EXPECT_GT(FileSize(), repaired_size);
  }
  {
    Wal wal(work_dir_);
    const auto result = wal.ScanAndRepair();
    ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
    EXPECT_FALSE(result.tail_truncated);
    EXPECT_EQ(result.frontier, 4u);
    ASSERT_EQ(result.records.size(), 3u);
    EXPECT_EQ(result.records[2].epoch, 4u);
  }
}

TEST_F(WalFrameTest, IncompleteTailHeaderIsTruncated) {
  AppendEpochs({1, 2});
  const off_t full_size = FileSize();

  // Simulate an append that stopped inside the next frame's header.
  const int fd = ::open(wal_path().c_str(), O_WRONLY | O_APPEND);
  ASSERT_GE(fd, 0);
  const uint8_t partial[4] = {0x4c, 0x57, 0x41, 0x4c};
  ASSERT_EQ(::write(fd, partial, sizeof(partial)), 4);
  ASSERT_EQ(::close(fd), 0);

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_TRUE(result.tail_truncated);
  EXPECT_EQ(result.frontier, 2u);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, MidFileCorruptionFailsWithoutTruncating) {
  AppendEpochs({1, 2, 3});
  const off_t full_size = FileSize();

  // Damage the first frame's payload, leaving valid frames after it.
  FlipByteAt(static_cast<off_t>(Wal::kHeaderSize) + 1);

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_FALSE(result.tail_truncated);
  EXPECT_EQ(result.frontier, 0u);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, OversizedPayloadLengthFailsWithoutTruncating) {
  AppendEpochs({1});
  const off_t full_size = FileSize();

  // payload_len sits at offset 8 and is rejected above 256 MiB.
  const int fd = ::open(wal_path().c_str(), O_WRONLY);
  ASSERT_GE(fd, 0);
  const uint8_t oversized[4] = {0x01, 0x00, 0x00, 0x20};  // 0x20000001
  ASSERT_EQ(::pwrite(fd, oversized, sizeof(oversized), 8), 4);
  ASSERT_EQ(::close(fd), 0);

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, EpochRegressionFailsWithoutTruncating) {
  // Two separate groups, the second carrying an older epoch than the first.
  {
    Wal wal(work_dir_);
    std::map<EpochNumber, LogRecords> buckets;
    buckets[3] = MakeRecords(3, "k3");
    ASSERT_TRUE(wal.AppendGroup(buckets, 3).ok);
  }
  {
    Wal wal(work_dir_);
    std::map<EpochNumber, LogRecords> buckets;
    buckets[2] = MakeRecords(2, "k2");
    ASSERT_TRUE(wal.AppendGroup(buckets, 2).ok);
  }
  const off_t full_size = FileSize();

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, WriteFailurePropagatesWithoutSyncing) {
  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  bool synced = false;
  io.write = [](int, const void*, size_t) -> ssize_t {
    errno = EIO;
    return -1;
  };
  io.fdatasync = [&synced](int) {
    synced = true;
    return 0;
  };

  Wal wal(work_dir_, io);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  const auto result = wal.AppendGroup(buckets, 1);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error_number, EIO);
  EXPECT_FALSE(synced);
}

TEST_F(WalFrameTest, FdatasyncFailurePropagates) {
  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  io.fdatasync = [](int) {
    errno = EIO;
    return -1;
  };

  Wal wal(work_dir_, io);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  const auto result = wal.AppendGroup(buckets, 1);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error_number, EIO);
}

TEST_F(WalFrameTest, EmptyGroupNeitherWritesNorSyncs) {
  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  bool wrote = false;
  bool synced = false;
  io.write = [&wrote](int fd, const void* data, size_t size) {
    wrote = true;
    return ::write(fd, data, size);
  };
  io.fdatasync = [&synced](int fd) {
    synced = true;
    return ::fdatasync(fd);
  };

  Wal wal(work_dir_, io);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[5] = MakeRecords(5, "k5");
  // Target below every bucket: nothing is eligible.
  const auto result = wal.AppendGroup(buckets, 4);
  EXPECT_TRUE(result.ok);
  EXPECT_FALSE(wrote);
  EXPECT_FALSE(synced);
  EXPECT_EQ(FileSize(), 0);
}

}  // namespace
