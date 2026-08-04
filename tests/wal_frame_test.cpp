#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <map>
#include <string>

#include <msgpack.hpp>

#include "recovery/crc32c.h"
#include "recovery/wal.h"

namespace {

using LineairDB::EpochNumber;
using LineairDB::Recovery::Crc32c;
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
    const bool read_ok = ::pread(fd, &byte, 1, offset) == 1;
    byte = static_cast<uint8_t>(byte ^ 0xffu);
    const bool write_ok = read_ok && ::pwrite(fd, &byte, 1, offset) == 1;
    ::close(fd);
    ASSERT_TRUE(read_ok);
    ASSERT_TRUE(write_ok);
  }

  void AppendEpochs(const std::vector<EpochNumber>& epochs) {
    Wal wal(work_dir_);
    ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
    std::map<EpochNumber, LogRecords> buckets;
    for (const auto epoch : epochs) {
      buckets[epoch] = MakeRecords(epoch, "k" + std::to_string(epoch));
    }
    const auto result = wal.AppendGroup(buckets, epochs.back());
    ASSERT_TRUE(result.ok) << "errno " << result.error_number;
  }

  // Appends raw bytes, standing in for the on-disk state a torn or corrupted
  // group write leaves behind.
  void AppendRawBytes(const std::vector<uint8_t>& bytes) {
    const int fd = ::open(wal_path().c_str(), O_WRONLY | O_APPEND);
    ASSERT_GE(fd, 0);
    const bool write_ok = ::write(fd, bytes.data(), bytes.size()) ==
                          static_cast<ssize_t>(bytes.size());
    ::close(fd);
    ASSERT_TRUE(write_ok);
  }

  static void PutLe16(std::vector<uint8_t>& out, uint16_t value) {
    out.push_back(static_cast<uint8_t>(value & 0xffu));
    out.push_back(static_cast<uint8_t>((value >> 8) & 0xffu));
  }

  static void PutLe32(std::vector<uint8_t>& out, uint32_t value) {
    out.push_back(static_cast<uint8_t>(value & 0xffu));
    out.push_back(static_cast<uint8_t>((value >> 8) & 0xffu));
    out.push_back(static_cast<uint8_t>((value >> 16) & 0xffu));
    out.push_back(static_cast<uint8_t>((value >> 24) & 0xffu));
  }

  // A syntactically complete header whose payload the file does not carry.
  std::vector<uint8_t> MakeHeaderClaimingPayload(EpochNumber epoch,
                                                 uint32_t payload_len) {
    std::vector<uint8_t> header;
    PutLe32(header, Wal::kMagic);
    PutLe16(header, Wal::kVersion);
    PutLe16(header, Wal::kFlags);
    PutLe32(header, payload_len);
    PutLe32(header, epoch);
    PutLe32(header, 0);  // The crc is never reached: the payload is missing.
    return header;
  }

  // A complete frame with a correct checksum over an arbitrary payload,
  // standing in for on-disk bytes that damage or an append produced. The
  // header fields default to valid values and can be overridden to build
  // frames the scan must reject on the field alone.
  std::vector<uint8_t> MakeFrame(EpochNumber epoch,
                                 const std::vector<uint8_t>& payload,
                                 uint32_t magic = Wal::kMagic,
                                 uint16_t version = Wal::kVersion,
                                 uint16_t flags = Wal::kFlags) {
    std::vector<uint8_t> frame;
    PutLe32(frame, magic);
    PutLe16(frame, version);
    PutLe16(frame, flags);
    PutLe32(frame, static_cast<uint32_t>(payload.size()));
    PutLe32(frame, epoch);
    Crc32c crc;
    crc.Update(frame.data(), 16);
    crc.Update(payload.data(), payload.size());
    PutLe32(frame, crc.Finish());
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
  }

  static std::vector<uint8_t> PackRecords(const LogRecords& records) {
    msgpack::sbuffer buffer;
    msgpack::pack(buffer, records);
    const auto* data = reinterpret_cast<const uint8_t*>(buffer.data());
    return std::vector<uint8_t>(data, data + buffer.size());
  }

  std::string root_;
  std::string work_dir_;
};

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

TEST_F(WalFrameTest, AGroupIsOneWriteAndOneSync) {
  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  int write_calls = 0;
  int sync_calls = 0;
  io.write = [&write_calls](int fd, const void* data, size_t size) {
    ++write_calls;
    return ::write(fd, data, size);
  };
  io.fdatasync = [&sync_calls](int fd) {
    ++sync_calls;
    return ::fdatasync(fd);
  };

  Wal wal(work_dir_, io);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  buckets[2] = MakeRecords(2, "k2");
  buckets[3] = MakeRecords(3, "k3");
  ASSERT_TRUE(wal.AppendGroup(buckets, 3).ok);
  EXPECT_EQ(write_calls, 1);
  EXPECT_EQ(sync_calls, 1);
}

TEST_F(WalFrameTest, GroupSkipsBucketsAboveTheTarget) {
  Wal wal(work_dir_);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
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

  // Damage the last payload byte of the final frame, which breaks only that
  // frame's checksum.
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
    ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
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

  // Simulate an append that stopped inside the next frame's header: the
  // little-endian encoding of kMagic, cut after four bytes.
  AppendRawBytes({0x4c, 0x41, 0x57, 0x4c});

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_TRUE(result.tail_truncated);
  EXPECT_EQ(result.frontier, 2u);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, AnIncompleteTailPayloadIsTruncated) {
  AppendEpochs({1, 2});
  const off_t full_size = FileSize();

  // A torn group write landed a full header claiming fifty payload bytes and
  // only ten of them.
  auto torn = MakeHeaderClaimingPayload(3, 50);
  torn.insert(torn.end(), 10, 0xab);
  AppendRawBytes(torn);

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_TRUE(result.tail_truncated);
  EXPECT_EQ(result.frontier, 2u);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, AnInflatedLengthWithAFrameBehindFailsWithoutTruncating) {
  AppendEpochs({1, 2, 3});
  const off_t full_size = FileSize();

  // Inflate the first frame's payload_len (offset 8) so its frame claims the
  // rest of the file; the intact frames behind it are evidence against a
  // tear.
  const int fd = ::open(wal_path().c_str(), O_WRONLY);
  ASSERT_GE(fd, 0);
  const uint8_t inflated[4] = {0x00, 0x00, 0x10, 0x00};  // 1 MiB
  const bool write_ok = ::pwrite(fd, inflated, sizeof(inflated), 8) == 4;
  ::close(fd);
  ASSERT_TRUE(write_ok);

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_FALSE(result.tail_truncated);
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
  const bool write_ok = ::pwrite(fd, oversized, sizeof(oversized), 8) == 4;
  ::close(fd);
  ASSERT_TRUE(write_ok);

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, AnAppendBelowTheFrontierIsRefused) {
  AppendEpochs({3});

  Wal wal(work_dir_);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[2] = MakeRecords(2, "k2");
  const auto result = wal.AppendGroup(buckets, 2);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error_number, EINVAL);
}

TEST_F(WalFrameTest, AnAppendBeforeTheScanIsRefused) {
  Wal wal(work_dir_);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  const auto result = wal.AppendGroup(buckets, 1);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error_number, EINVAL);
  EXPECT_EQ(FileSize(), 0);
}

TEST_F(WalFrameTest, EpochRegressionOnDiskFailsWithoutTruncating) {
  AppendEpochs({3});
  // Damage wrote a checksum-valid frame carrying an older epoch.
  AppendRawBytes(MakeFrame(2, PackRecords(MakeRecords(2, "k2"))));
  const off_t full_size = FileSize();

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, AZeroEpochFrameFailsWithoutTruncating) {
  { Wal wal(work_dir_); }  // create the file
  AppendRawBytes(MakeFrame(0, PackRecords(MakeRecords(0, "k0"))));
  const off_t full_size = FileSize();

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, ARecordDisagreeingWithItsFrameFailsWithoutTruncating) {
  AppendEpochs({1});
  // The frame says epoch 2; the record inside says epoch 7.
  AppendRawBytes(MakeFrame(2, PackRecords(MakeRecords(7, "k7"))));
  const off_t full_size = FileSize();

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, ATornTailEmbeddingAFrameImageFailsStop) {
  AppendEpochs({1});

  // A torn frame whose recorded payload embeds a byte-exact intact frame,
  // records and all. The probe cannot tell it from damage in an
  // already-synced region, and ambiguity resolves toward fail-stop, never
  // toward repair.
  const auto embedded = MakeFrame(5, PackRecords(MakeRecords(5, "k5")));
  auto torn = MakeHeaderClaimingPayload(6, 4096);
  torn.insert(torn.end(), embedded.begin(), embedded.end());
  AppendRawBytes(torn);
  const off_t full_size = FileSize();

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, AFramePayloadThatDoesNotDecodeFailsWithoutTruncating) {
  AppendEpochs({1});
  // Checksum-valid, but the payload is not a record list. The reject fires
  // on the decoded content, even for the final frame of the file.
  AppendRawBytes(MakeFrame(2, {0x01, 0x02, 0x03}));
  const off_t full_size = FileSize();

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, AFrameWithTrailingPayloadBytesFailsWithoutTruncating) {
  AppendEpochs({1});
  auto padded = PackRecords(MakeRecords(2, "k2"));
  padded.push_back(0x00);
  AppendRawBytes(MakeFrame(2, padded));
  const off_t full_size = FileSize();

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, AFrameCarryingNoRecordFailsWithoutTruncating) {
  AppendEpochs({1});
  AppendRawBytes(MakeFrame(2, PackRecords(LogRecords{})));
  const off_t full_size = FileSize();

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, AHeaderFieldAnomalyFailsWithoutTruncating) {
  const auto payload = PackRecords(MakeRecords(1, "k1"));
  const struct {
    uint32_t magic;
    uint16_t version;
    uint16_t flags;
  } cases[] = {
      {0x4c414c57u, Wal::kVersion, Wal::kFlags},  // wrong magic
      {Wal::kMagic, 2, Wal::kFlags},              // unsupported version
      {Wal::kMagic, Wal::kVersion, 1},            // unknown flags
  };
  for (const auto& anomaly : cases) {
    TearDown();  // fresh directory per case; the fixture removes the last one
    SetUp();
    { Wal wal(work_dir_); }
    AppendRawBytes(
        MakeFrame(1, payload, anomaly.magic, anomaly.version, anomaly.flags));
    const off_t full_size = FileSize();

    Wal wal(work_dir_);
    const auto result = wal.ScanAndRepair();
    EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
    EXPECT_EQ(FileSize(), full_size);
  }
}

TEST_F(WalFrameTest, AFrameStraddlingTheProbeWindowBoundaryIsFound) {
  AppendEpochs({1});
  const off_t damage = FileSize();

  // The probe reads 4 MiB windows starting one byte past the damage. Place
  // an intact frame so its header straddles the first window boundary; the
  // windows overlap by one byte less than a header, so the second window
  // must find it.
  constexpr uint64_t kWindow = 4u * 1024u * 1024u;
  const auto frame = MakeFrame(5, PackRecords(MakeRecords(5, "k5")));
  const uint64_t frame_at = kWindow + 1 - 10;  // relative to the damage
  std::vector<uint8_t> torn = MakeHeaderClaimingPayload(6, 8u * 1024u * 1024u);
  torn.resize(frame_at, 0x00);
  torn.insert(torn.end(), frame.begin(), frame.end());
  AppendRawBytes(torn);

  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), damage + static_cast<off_t>(torn.size()));
}

TEST_F(WalFrameTest, ShortWritesAreRetriedUntilTheGroupIsComplete) {
  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  int write_calls = 0;
  io.write = [&write_calls](int fd, const void* data, size_t) -> ssize_t {
    ++write_calls;
    return ::write(fd, data, 1);  // one byte per call
  };

  Wal wal(work_dir_, io);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  ASSERT_TRUE(wal.AppendGroup(buckets, 1).ok);
  EXPECT_GT(write_calls, 1);

  Wal reader(work_dir_);
  const auto result = reader.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_EQ(result.frontier, 1u);
  ASSERT_EQ(result.records.size(), 1u);
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
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  const auto result = wal.AppendGroup(buckets, 1);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error_number, EIO);
  EXPECT_FALSE(synced);
}

TEST_F(WalFrameTest, AFailedAppendRefusesEveryLaterAppend) {
  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  int write_calls = 0;
  io.write = [&write_calls](int, const void*, size_t) -> ssize_t {
    ++write_calls;
    errno = EIO;
    return -1;
  };

  Wal wal(work_dir_, io);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  ASSERT_FALSE(wal.AppendGroup(buckets, 1).ok);
  const int calls_after_failure = write_calls;

  // The file may end in a torn frame; the instance must not write past it.
  const auto second = wal.AppendGroup(buckets, 1);
  EXPECT_FALSE(second.ok);
  EXPECT_EQ(write_calls, calls_after_failure);
}

TEST_F(WalFrameTest, ABucketTheScanWouldRejectIsRefused) {
  Wal wal(work_dir_);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  {
    std::map<EpochNumber, LogRecords> buckets;
    buckets[1] = LogRecords{};  // empty
    const auto result = wal.AppendGroup(buckets, 1);
    EXPECT_FALSE(result.ok);
    EXPECT_EQ(result.error_number, EINVAL);
  }
  {
    std::map<EpochNumber, LogRecords> buckets;
    buckets[2] = MakeRecords(7, "k7");  // record epoch disagrees
    const auto result = wal.AppendGroup(buckets, 2);
    EXPECT_FALSE(result.ok);
    EXPECT_EQ(result.error_number, EINVAL);
  }
  EXPECT_EQ(FileSize(), 0);
}

TEST_F(WalFrameTest, FdatasyncFailurePropagates) {
  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  io.fdatasync = [](int) {
    errno = EIO;
    return -1;
  };

  Wal wal(work_dir_, io);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  const auto result = wal.AppendGroup(buckets, 1);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error_number, EIO);
}

TEST_F(WalFrameTest, ScanAcceptsTheMaximumEpoch) {
  // The scanner does not bound the epoch; the resume-epoch computation is what
  // has to refuse near the wrap. Recording that division here keeps a later
  // change from quietly moving the check into the scanner and leaving startup
  // to add one to UINT32_MAX.
  const EpochNumber near_wrap = 0xFFFFFFFFu;
  {
    Wal wal(work_dir_);
    ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
    std::map<EpochNumber, LogRecords> buckets;
    buckets[near_wrap] = MakeRecords(near_wrap, "k");
    ASSERT_TRUE(wal.AppendGroup(buckets, near_wrap).ok);
  }
  Wal wal(work_dir_);
  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok);
  EXPECT_EQ(result.frontier, near_wrap);
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
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
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
