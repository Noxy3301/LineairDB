#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <map>
#include <string>

#include <msgpack.hpp>

#include "recovery/crc32c.h"
#include "recovery/wal.h"

namespace {

using LineairDB::EpochNumber;
using LineairDB::Recovery::ComputeCrc32c;
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

  /**
   * Where the log ends, which is not where the file ends: capacity beyond
   * the last frame is written out with zeroes in advance.
   */
  off_t EndOfLog() {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
    EXPECT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
    return wal.write_offset();
  }

  // Flips a bit at an absolute offset, standing in for a torn or damaged
  // write.
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

  // Asserts every byte in [from, to) reads back zero: repair is expected to
  // have overwritten this range, not merely to have stopped trusting it.
  void AssertRangeIsZero(off_t from, off_t to) {
    const int fd = ::open(wal_path().c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    std::vector<uint8_t> chunk(1 << 16);
    for (off_t at = from; at < to;) {
      const size_t want =
          static_cast<size_t>(std::min<off_t>(chunk.size(), to - at));
      ASSERT_EQ(::pread(fd, chunk.data(), want, at),
                static_cast<ssize_t>(want));
      for (size_t i = 0; i < want; ++i) {
        ASSERT_EQ(chunk[i], 0) << "offset " << (at + static_cast<off_t>(i));
      }
      at += static_cast<off_t>(want);
    }
    ASSERT_EQ(::close(fd), 0);
  }

  /**
   * Rewrites the payload length of the frame at `frame_offset`, leaving the
   * checksum stale. Stands in for damage to the one field that says how far
   * the frame reaches, which is the field a repair must not take on trust.
   */
  void SetPayloadLengthAt(off_t frame_offset, uint32_t length) {
    const int fd = ::open(wal_path().c_str(), O_WRONLY);
    ASSERT_GE(fd, 0);
    const uint8_t bytes[4] = {
        static_cast<uint8_t>(length & 0xffu),
        static_cast<uint8_t>((length >> 8) & 0xffu),
        static_cast<uint8_t>((length >> 16) & 0xffu),
        static_cast<uint8_t>((length >> 24) & 0xffu)};
    ASSERT_EQ(::pwrite(fd, bytes, sizeof(bytes), frame_offset + 8), 4);
    ASSERT_EQ(::close(fd), 0);
  }

  /** Offset one past the frame at `frame_offset`, taken from its own
   * length. */
  off_t FrameEnd(off_t frame_offset) {
    uint8_t header[Wal::kHeaderSize];
    const int fd = ::open(wal_path().c_str(), O_RDONLY);
    EXPECT_GE(fd, 0);
    EXPECT_EQ(::pread(fd, header, sizeof(header), frame_offset),
              static_cast<ssize_t>(sizeof(header)));
    EXPECT_EQ(::close(fd), 0);
    const uint32_t length = static_cast<uint32_t>(header[8]) |
                            (static_cast<uint32_t>(header[9]) << 8) |
                            (static_cast<uint32_t>(header[10]) << 16) |
                            (static_cast<uint32_t>(header[11]) << 24);
    return frame_offset + static_cast<off_t>(Wal::kHeaderSize) + length;
  }

  void AppendEpochs(const std::vector<EpochNumber>& epochs) {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
    ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
    std::map<EpochNumber, LogRecords> buckets;
    for (const auto epoch : epochs) {
      buckets[epoch] = MakeRecords(epoch, "k" + std::to_string(epoch));
    }
    const auto result = wal.AppendGroup(buckets, epochs.back());
    ASSERT_TRUE(result.ok) << "errno " << result.error_number;
    ASSERT_EQ(wal.extension_count(), 0u);
  }

  // Writes raw bytes at an absolute offset, standing in for the on-disk
  // state a torn or corrupted group write leaves behind. A settled-size log
  // must be addressed by offset rather than by appending: the file's own end
  // is capacity, not the log's end.
  void WriteRawBytesAt(off_t offset, const std::vector<uint8_t>& bytes) {
    const int fd = ::open(wal_path().c_str(), O_WRONLY);
    ASSERT_GE(fd, 0);
    const bool write_ok =
        ::pwrite(fd, bytes.data(), bytes.size(), offset) ==
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

  // A syntactically complete header whose payload the file does not fully
  // carry: the remaining declared bytes read back as whatever the log's
  // reserved capacity already held.
  std::vector<uint8_t> MakeHeaderClaimingPayload(EpochNumber epoch,
                                                 uint32_t payload_len) {
    std::vector<uint8_t> header;
    PutLe32(header, Wal::kMagic);
    PutLe16(header, Wal::kVersion);
    PutLe16(header, Wal::kFlags);
    PutLe32(header, payload_len);
    PutLe32(header, epoch);
    PutLe32(header, 0);  // A placeholder; the real payload was never written.
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

  // Enough for every group these tests write, and small enough that writing
  // it out costs nothing worth measuring.
  static constexpr uint64_t kCapacity = 1ull << 20;

  std::string root_;
  std::string work_dir_;
};

TEST_F(WalFrameTest, Crc32cKnownVector) {
  EXPECT_EQ(ComputeCrc32c("123456789", 9), 0xE3069283u);
}

TEST_F(WalFrameTest, ScanOfAFreshLogHasNoFrontier) {
  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Ok);
  EXPECT_EQ(result.frontier, 0u);
  EXPECT_TRUE(result.records.empty());
  EXPECT_FALSE(result.tail_truncated);
}

TEST_F(WalFrameTest, ScanReturnsTheLastCompleteEpoch) {
  AppendEpochs({1, 3});

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
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
  io.pwrite = [&write_calls](int fd, const void* data, size_t size,
                             off_t offset) {
    ++write_calls;
    return ::pwrite(fd, data, size, offset);
  };
  io.fdatasync = [&sync_calls](int fd) {
    ++sync_calls;
    return ::fdatasync(fd);
  };

  Wal wal(work_dir_, io, kCapacity);
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
  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
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

TEST_F(WalFrameTest, TailCorruptionIsRepairedAndLaterGroupsRecover) {
  AppendEpochs({1, 2, 3});
  const off_t full_end = EndOfLog();

  // Damage the checksum of the last frame only.
  FlipByteAt(full_end - 1);

  off_t repaired_end = 0;
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
    const auto result = wal.ScanAndRepair();
    ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
    EXPECT_TRUE(result.tail_truncated);
    EXPECT_EQ(result.frontier, 2u);
    ASSERT_EQ(result.records.size(), 2u);
    repaired_end = wal.write_offset();
    EXPECT_LT(repaired_end, full_end);
  }

  // The repaired log must accept further groups and read back cleanly, and
  // its file size must not have moved: repair zeroes bytes, it never
  // shrinks the file.
  const off_t file_size_before = FileSize();
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
    ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
    ASSERT_EQ(wal.write_offset(), repaired_end);
    EXPECT_EQ(FileSize(), file_size_before);
    std::map<EpochNumber, LogRecords> buckets;
    buckets[4] = MakeRecords(4, "k4");
    ASSERT_TRUE(wal.AppendGroup(buckets, 4).ok);
    EXPECT_GT(wal.write_offset(), repaired_end);
    EXPECT_EQ(FileSize(), file_size_before);
  }
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
    const auto result = wal.ScanAndRepair();
    ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
    EXPECT_FALSE(result.tail_truncated);
    EXPECT_EQ(result.frontier, 4u);
    ASSERT_EQ(result.records.size(), 3u);
    EXPECT_EQ(result.records[2].epoch, 4u);
  }
}

TEST_F(WalFrameTest, PartialHeaderIsRepaired) {
  AppendEpochs({1, 2});
  const off_t full_end = EndOfLog();
  const off_t full_size = FileSize();

  // A write that stopped inside the next frame's header, which leaves a
  // prefix of the magic where the zeroes used to be.
  const std::vector<uint8_t> partial_header = {0x4c, 0x41, 0x57};
  WriteRawBytesAt(full_end, partial_header);

  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
    const auto result = wal.ScanAndRepair();
    ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
    EXPECT_TRUE(result.tail_truncated);
    EXPECT_EQ(result.frontier, 2u);
    EXPECT_EQ(wal.write_offset(), full_end);
  }
  // The bytes the torn write left behind are restored to zero, not just
  // logically ignored, and the file did not shrink.
  AssertRangeIsZero(full_end,
                    full_end + static_cast<off_t>(partial_header.size()));
  EXPECT_EQ(FileSize(), full_size);

  // A later scan reaches the same end with nothing left to repair.
  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_FALSE(result.tail_truncated);
  EXPECT_EQ(wal.write_offset(), full_end);
}

TEST_F(WalFrameTest, PartialPayloadIsRepaired) {
  AppendEpochs({1, 2});
  const off_t full_end = EndOfLog();
  const off_t full_size = FileSize();

  // A torn group write landed a full header claiming fifty payload bytes and
  // only ten of them; the rest read back as the zeroes of unused capacity.
  auto torn = MakeHeaderClaimingPayload(3, 50);
  torn.insert(torn.end(), 10, 0xab);
  WriteRawBytesAt(full_end, torn);

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_TRUE(result.tail_truncated);
  EXPECT_EQ(result.frontier, 2u);
  EXPECT_EQ(wal.write_offset(), full_end);
  EXPECT_EQ(FileSize(), full_size);
}

// A header whose constant fields arrived whole cannot be the remains of a
// write that stopped partway, so it is damage even with nothing written
// after it. Put at the end of the log, where an interrupted write would
// have left its bytes.
TEST_F(WalFrameTest, WholeHeaderWithUnknownFlagsAtTheLogEndFails) {
  AppendEpochs({1});
  const off_t log_end = EndOfLog();

  const std::vector<uint8_t> header = {0x4c, 0x41, 0x57, 0x4c, 0x01, 0x00,
                                       0x07, 0x00};
  WriteRawBytesAt(log_end, header);

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_FALSE(result.tail_truncated);
}

// The same header inside the log rather than at its end.
TEST_F(WalFrameTest, WholeHeaderWithUnknownFlagsMidLogFails) {
  AppendEpochs({1});

  WriteRawBytesAt(6, {0x07, 0x00});

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_FALSE(result.tail_truncated);
}

/**
 * A length corrupted upwards must not be allowed to define the region a
 * repair may erase.
 *
 * The frame at offset 0 claims to run to the end of the log, so every frame
 * that follows falls inside its declared extent and the bytes after that
 * extent are the zeroes of unused capacity. A repair judged by "is anything
 * written past where this frame ends" would accept it and zero the whole
 * log; the frames that follow were acknowledged as durable.
 */
TEST_F(WalFrameTest, ALengthThatSwallowsLaterFramesFails) {
  AppendEpochs({1, 2, 3});
  const off_t log_end = EndOfLog();
  const off_t file_size = FileSize();

  SetPayloadLengthAt(0, static_cast<uint32_t>(log_end - Wal::kHeaderSize));

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt) << result.detail;
  EXPECT_FALSE(result.tail_truncated);
  EXPECT_EQ(result.frontier, 0u);
  EXPECT_EQ(wal.write_offset(), 0);
  EXPECT_EQ(FileSize(), file_size);
}

// The same corruption taken past the end of the file, where no bound
// derived from the length can reject anything at all.
TEST_F(WalFrameTest, ALengthPastTheFileWithLaterFramesFails) {
  AppendEpochs({1, 2, 3});
  const off_t file_size = FileSize();

  SetPayloadLengthAt(0, static_cast<uint32_t>(file_size + 4096));

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt) << result.detail;
  EXPECT_FALSE(result.tail_truncated);
  EXPECT_EQ(FileSize(), file_size);
}

// The last frame's length corrupted upwards, with nothing surviving beyond
// it, is indistinguishable from a write that stopped inside a large frame.
TEST_F(WalFrameTest, ALengthCorruptedOnTheLastFrameIsRepaired) {
  AppendEpochs({1, 2});
  const off_t log_end = EndOfLog();
  ASSERT_GT(log_end, static_cast<off_t>(Wal::kHeaderSize) * 2);

  const off_t second = FrameEnd(0);
  SetPayloadLengthAt(second, 4096);

  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
    const auto result = wal.ScanAndRepair();
    ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
    EXPECT_TRUE(result.tail_truncated);
    EXPECT_EQ(result.frontier, 1u);
    EXPECT_EQ(wal.write_offset(), second);
  }
  // The second frame's real bytes, not just its now-disowned length claim,
  // are restored to zero.
  AssertRangeIsZero(second, log_end);

  // A later scan finds nothing left to repair.
  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_FALSE(result.tail_truncated);
  EXPECT_EQ(wal.write_offset(), second);
}

// A complete frame that fails its checksum is the interrupted tail only if
// it is also the last thing written. Non-frame junk sitting past its own
// declared end proves something was written after it, so the damage sits
// among frames older appends already synced, and repair must not zero over
// it just because no intact frame happens to be findable there either.
TEST_F(WalFrameTest, AChecksumBrokenFrameFollowedByJunkFailsWithoutRepairing) {
  AppendEpochs({1});
  const off_t frame_end = FrameEnd(0);

  // Break the checksum without changing the frame's declared length.
  FlipByteAt(static_cast<off_t>(Wal::kHeaderSize) + 1);
  // Junk that is neither a frame nor zero, placed right at the frame's own
  // end.
  const std::vector<uint8_t> junk(4, 0x55);
  WriteRawBytesAt(frame_end, junk);
  const off_t full_size = FileSize();

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_NE(result.detail.find("with data beyond the frame"),
           std::string::npos)
      << result.detail;
  EXPECT_EQ(FileSize(), full_size);

  // The failed scan must not have touched the file: the junk reads back
  // unchanged.
  const int fd = ::open(wal_path().c_str(), O_RDONLY);
  ASSERT_GE(fd, 0);
  uint8_t readback[4] = {0, 0, 0, 0};
  ASSERT_EQ(::pread(fd, readback, sizeof(readback), frame_end),
            static_cast<ssize_t>(sizeof(readback)));
  ASSERT_EQ(::close(fd), 0);
  EXPECT_EQ(std::memcmp(readback, junk.data(), junk.size()), 0);
}

TEST_F(WalFrameTest, MidLogCorruptionFailsWithoutRepairing) {
  AppendEpochs({1, 2, 3});
  const off_t full_size = FileSize();

  // Damage the first frame's payload, leaving valid frames after it.
  FlipByteAt(static_cast<off_t>(Wal::kHeaderSize) + 1);

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_FALSE(result.tail_truncated);
  EXPECT_EQ(result.frontier, 0u);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, OversizedPayloadLengthFailsWithoutRepairing) {
  AppendEpochs({1});
  const off_t full_size = FileSize();

  // payload_len sits at offset 8 and is rejected above 256 MiB.
  const uint8_t oversized[4] = {0x01, 0x00, 0x00, 0x20};  // 0x20000001
  WriteRawBytesAt(8, std::vector<uint8_t>(oversized, oversized + 4));

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, AnAppendBelowTheFrontierIsRefused) {
  AppendEpochs({3});

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[2] = MakeRecords(2, "k2");
  const auto result = wal.AppendGroup(buckets, 2);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error_number, EINVAL);
}

/**
 * The rule that nothing reaches the log before its end is known holds even
 * for an instance that was never scanned at all.
 *
 * The expected output is empty because the reason is logged through spdlog,
 * which writes to stdout, while a death test watches stderr.
 */
TEST_F(WalFrameTest, AnAppendBeforeTheScanIsRefused) {
  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  EXPECT_DEATH(wal.AppendGroup(buckets, 1), "");
}

TEST_F(WalFrameTest, EpochRegressionOnDiskFailsWithoutRepairing) {
  AppendEpochs({3});
  const off_t log_end = EndOfLog();
  // A legitimate AppendGroup call can no longer produce this: the frontier
  // check refuses a regressed epoch before any write. Forged directly on
  // disk, standing in for a build without that check, or for corruption.
  WriteRawBytesAt(log_end, MakeFrame(2, PackRecords(MakeRecords(2, "k2"))));
  const off_t full_size = FileSize();

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, AZeroEpochFrameFailsWithoutRepairing) {
  const off_t log_end = EndOfLog();  // creates the file and its capacity
  WriteRawBytesAt(log_end, MakeFrame(0, PackRecords(MakeRecords(0, "k0"))));
  const off_t full_size = FileSize();

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, ARecordDisagreeingWithItsFrameFailsWithoutRepairing) {
  AppendEpochs({1});
  const off_t log_end = EndOfLog();
  // The frame says epoch 2; the record inside says epoch 7.
  WriteRawBytesAt(log_end, MakeFrame(2, PackRecords(MakeRecords(7, "k7"))));
  const off_t full_size = FileSize();

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, ATornTailEmbeddingAFrameImageFailsStop) {
  AppendEpochs({1});
  const off_t log_end = EndOfLog();

  // A torn frame whose recorded payload embeds a byte-exact intact frame,
  // records and all. The search cannot tell it from damage in an
  // already-synced region, and ambiguity resolves toward fail-stop, never
  // toward repair.
  const auto embedded = MakeFrame(5, PackRecords(MakeRecords(5, "k5")));
  auto torn = MakeHeaderClaimingPayload(6, 4096);
  torn.insert(torn.end(), embedded.begin(), embedded.end());
  WriteRawBytesAt(log_end, torn);
  const off_t full_size = FileSize();

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

/**
 * A record's value may hold anything, including the bytes of a frame that
 * would pass its own checksum. Inside the payload of a frame that is itself
 * broken, such bytes are indistinguishable from a frame that survived the
 * damage.
 *
 * The choice made here is to fail-stop: refusing to start is recoverable by
 * hand, whereas erasing what might be an acknowledged frame is not.
 */
TEST_F(WalFrameTest, AFrameForgedInsideAPayloadIsFailedOn) {
  const auto frame = MakeFrame(9, PackRecords(MakeRecords(9, "forged")));
  const std::string forged(frame.begin(), frame.end());

  // Committed as the value of a record, then the frame that carries it is
  // broken.
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
    ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
    LogRecords records = MakeRecords(1, "carrier");
    records[0].key_value_pairs[0].buffer = forged;
    std::map<EpochNumber, LogRecords> buckets;
    buckets[1] = std::move(records);
    ASSERT_TRUE(wal.AppendGroup(buckets, 1).ok);
  }
  FlipByteAt(static_cast<off_t>(Wal::kHeaderSize) + 1);

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt) << result.detail;
  EXPECT_FALSE(result.tail_truncated);
}

TEST_F(WalFrameTest, AFramePayloadThatDoesNotDecodeFailsWithoutRepairing) {
  AppendEpochs({1});
  const off_t log_end = EndOfLog();
  // Checksum-valid, but the payload is not a record list. The reject fires
  // on the decoded content, even for the final frame of the file.
  WriteRawBytesAt(log_end, MakeFrame(2, {0x01, 0x02, 0x03}));
  const off_t full_size = FileSize();

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, AFrameWithTrailingPayloadBytesFailsWithoutRepairing) {
  AppendEpochs({1});
  const off_t log_end = EndOfLog();
  auto padded = PackRecords(MakeRecords(2, "k2"));
  padded.push_back(0x00);
  WriteRawBytesAt(log_end, MakeFrame(2, padded));
  const off_t full_size = FileSize();

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, AFrameCarryingNoRecordFailsWithoutRepairing) {
  AppendEpochs({1});
  const off_t log_end = EndOfLog();
  WriteRawBytesAt(log_end, MakeFrame(2, PackRecords(LogRecords{})));
  const off_t full_size = FileSize();

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(FileSize(), full_size);
}

TEST_F(WalFrameTest, AHeaderFieldAnomalyFailsWithoutRepairing) {
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
    const off_t log_end = EndOfLog();
    WriteRawBytesAt(
        log_end,
        MakeFrame(1, payload, anomaly.magic, anomaly.version, anomaly.flags));
    const off_t full_size = FileSize();

    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
    const auto result = wal.ScanAndRepair();
    EXPECT_EQ(result.status, WalScanResult::Status::Corrupt);
    EXPECT_EQ(FileSize(), full_size);
  }
}

// The search reads in 1 MiB chunks starting one byte past the damage, so its
// first chunk covers [1, 1 + 1 MiB) and the boundary sits at 1 + 1 MiB. A
// magic that begins two bytes before that has its last two bytes in the next
// chunk, and is found only because the chunks overlap by the length of a
// magic less one. Nothing valid is placed before it, so a search that missed
// it would find nothing and repair.
TEST_F(WalFrameTest, AFrameStraddlingTheProbeWindowBoundaryIsFound) {
  constexpr uint64_t kWideCapacity = 4ull << 20;
  constexpr off_t kBoundary = 1 + (1 << 20);

  // One frame long enough to carry the boundary inside its own payload, so
  // that the log ends past it and the reservation covers what follows.
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kWideCapacity);
    ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
    std::map<EpochNumber, LogRecords> buckets;
    buckets[1] = MakeRecords(1, std::string(2 << 20, 'x'));
    ASSERT_TRUE(wal.AppendGroup(buckets, 1).ok);
    ASSERT_GT(wal.write_offset(), kBoundary + 64);
  }

  // Break that frame, then place a whole frame with its magic across the
  // boundary.
  FlipByteAt(static_cast<off_t>(Wal::kHeaderSize) + 1);
  const auto survivor = MakeFrame(7, PackRecords(MakeRecords(7, "survivor")));
  const off_t placed = kBoundary - 2;
  WriteRawBytesAt(placed, survivor);

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kWideCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::Corrupt) << result.detail;
  EXPECT_FALSE(result.tail_truncated);
}

/**
 * A read that fails while checking whether a frame survives beyond the
 * damage must stop the scan.
 *
 * Treating it as "no frame there" would license the repair, and the repair
 * erases the bytes it was asking about. A transient read failure is not
 * evidence that the frames it could not read are absent.
 *
 * The candidate is placed inside the broken frame's own claimed extent
 * rather than past it: data past the extent is now settled by its mere
 * presence (AChecksumBrokenFrameFollowedByJunkFailsWithoutRepairing), so a
 * search only remains reachable, and its own read failures only remain
 * observable, for a candidate the frame's length claims as its own.
 */
TEST_F(WalFrameTest, AReadFailureWhileLookingForSurvivorsStopsTheScan) {
  AppendEpochs({1});
  const off_t log_end = EndOfLog();

  // A torn header claiming a payload large enough to hold a whole frame
  // image within its own extent, with that image placed a few bytes in.
  const auto embedded = MakeFrame(5, PackRecords(MakeRecords(5, "k5")));
  const off_t embedded_at = log_end + static_cast<off_t>(Wal::kHeaderSize) + 8;
  auto torn = MakeHeaderClaimingPayload(6, 4096);
  torn.resize(static_cast<size_t>(embedded_at - log_end), 0x00);
  torn.insert(torn.end(), embedded.begin(), embedded.end());
  WriteRawBytesAt(log_end, torn);
  const off_t file_size = FileSize();

  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  auto real_pread = io.pread;
  io.pread = [real_pread, embedded_at](int fd, void* data, size_t size,
                                       off_t offset) -> ssize_t {
    if (offset == embedded_at && size == Wal::kHeaderSize) {
      errno = EIO;
      return -1;
    }
    return real_pread(fd, data, size, offset);
  };

  Wal wal(work_dir_, io, kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::IoError) << result.detail;
  EXPECT_EQ(result.error_number, EIO);
  EXPECT_FALSE(result.tail_truncated);
  EXPECT_EQ(wal.write_offset(), 0);
  EXPECT_EQ(FileSize(), file_size);
}

TEST_F(WalFrameTest, ShortWritesAreRetriedUntilTheGroupIsComplete) {
  {
    LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
    int write_calls = 0;
    io.pwrite = [&write_calls](int fd, const void* data, size_t,
                               off_t offset) -> ssize_t {
      ++write_calls;
      return ::pwrite(fd, data, 1, offset);  // one byte per call
    };

    Wal wal(work_dir_, io, kCapacity);
    ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
    std::map<EpochNumber, LogRecords> buckets;
    buckets[1] = MakeRecords(1, "k1");
    ASSERT_TRUE(wal.AppendGroup(buckets, 1).ok);
    EXPECT_GT(write_calls, 1);
  }

  // The exclusive flock is held only while `wal` is alive; the reader needs
  // its own instance, opened after that one is gone.
  Wal reader(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = reader.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_EQ(result.frontier, 1u);
  ASSERT_EQ(result.records.size(), 1u);
}

// A pwrite is allowed to write less than it was asked for. The group has to
// be carried to completion at the right offsets, not restarted or left
// short.
TEST_F(WalFrameTest, APartialWriteIsCarriedToCompletion) {
  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  size_t calls = 0;
  io.pwrite = [&calls](int fd, const void* data, size_t size, off_t offset) {
    ++calls;
    return ::pwrite(fd, data, std::min<size_t>(size, 7), offset);
  };

  Wal wal(work_dir_, io, kCapacity);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  buckets[2] = MakeRecords(2, "k2");
  ASSERT_TRUE(wal.AppendGroup(buckets, 2).ok);
  EXPECT_GT(calls, 1u);

  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_FALSE(result.tail_truncated);
  EXPECT_EQ(result.frontier, 2u);
  EXPECT_EQ(result.records.size(), 2u);
}

TEST_F(WalFrameTest, WriteFailurePropagatesWithoutSyncing) {
  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  bool synced = false;
  io.pwrite = [](int, const void*, size_t, off_t) -> ssize_t {
    errno = EIO;
    return -1;
  };
  io.fdatasync = [&synced](int) {
    synced = true;
    return 0;
  };

  Wal wal(work_dir_, io, kCapacity);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  const auto result = wal.AppendGroup(buckets, 1);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error_number, EIO);
  EXPECT_FALSE(synced);
}

// After a group whose own outcome is unknown, where the log ends is unknown
// too, and every later group is refused rather than written at a guessed
// offset.
//
// The expected output is empty because the reason is logged through spdlog,
// which writes to stdout, while a death test watches stderr.
TEST_F(WalFrameTest, AFailedAppendRefusesEveryLaterAppend) {
  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  io.fdatasync = [](int) {
    errno = EIO;
    return -1;
  };

  Wal wal(work_dir_, io, kCapacity);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  std::map<EpochNumber, LogRecords> first;
  first[1] = MakeRecords(1, "k1");
  ASSERT_FALSE(wal.AppendGroup(first, 1).ok);

  std::map<EpochNumber, LogRecords> second;
  second[2] = MakeRecords(2, "k2");
  EXPECT_DEATH(wal.AppendGroup(second, 2), "");
}

TEST_F(WalFrameTest, ABucketTheScanWouldRejectIsRefused) {
  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
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
  EXPECT_EQ(wal.write_offset(), 0);
}

TEST_F(WalFrameTest, FdatasyncFailurePropagates) {
  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  io.fdatasync = [](int) {
    errno = EIO;
    return -1;
  };

  Wal wal(work_dir_, io, kCapacity);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  const auto result = wal.AppendGroup(buckets, 1);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error_number, EIO);
}

TEST_F(WalFrameTest, ScanAcceptsTheMaximumEpoch) {
  // The scanner does not bound the epoch; the resume-epoch computation is
  // what has to refuse near the wrap. Recording that division here keeps a
  // later change from quietly moving the check into the scanner and leaving
  // startup to add one to UINT32_MAX.
  const EpochNumber near_wrap = 0xFFFFFFFFu;
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
    ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
    std::map<EpochNumber, LogRecords> buckets;
    buckets[near_wrap] = MakeRecords(near_wrap, "k");
    ASSERT_TRUE(wal.AppendGroup(buckets, near_wrap).ok);
  }
  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok);
  EXPECT_EQ(result.frontier, near_wrap);
}

// The whole point of the design: the file's size is settled before the
// first group, so a group flush has no new size to persist.
TEST_F(WalFrameTest, CapacityIsWrittenOutAndGroupsDoNotChangeTheFileSize) {
  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  ASSERT_EQ(FileSize(), static_cast<off_t>(kCapacity));

  off_t previous_end = 0;
  for (EpochNumber epoch = 1; epoch <= 4; ++epoch) {
    std::map<EpochNumber, LogRecords> buckets;
    buckets[epoch] = MakeRecords(epoch, "k" + std::to_string(epoch));
    ASSERT_TRUE(wal.AppendGroup(buckets, epoch).ok);
    EXPECT_EQ(FileSize(), static_cast<off_t>(kCapacity));
    EXPECT_GT(wal.write_offset(), previous_end);
    previous_end = wal.write_offset();
  }
  EXPECT_EQ(wal.extension_count(), 0u);
}

// A log written before capacity existed ends where the file ends, with no
// zeroes after it. Opening it must find that end and reserve from there.
TEST_F(WalFrameTest, AGrownLogIsAdoptedWithoutLosingFrames) {
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(),
            Wal::kNoPreallocation);
    ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
    std::map<EpochNumber, LogRecords> buckets;
    buckets[1] = MakeRecords(1, "k1");
    buckets[2] = MakeRecords(2, "k2");
    ASSERT_TRUE(wal.AppendGroup(buckets, 2).ok);
    ASSERT_EQ(FileSize(), wal.write_offset());
  }
  const off_t grown_end = FileSize();

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_FALSE(result.tail_truncated);
  EXPECT_EQ(result.frontier, 2u);
  EXPECT_EQ(result.records.size(), 2u);
  EXPECT_EQ(wal.write_offset(), grown_end);
  EXPECT_EQ(FileSize(), static_cast<off_t>(kCapacity));

  std::map<EpochNumber, LogRecords> buckets;
  buckets[3] = MakeRecords(3, "k3");
  ASSERT_TRUE(wal.AppendGroup(buckets, 3).ok);
  EXPECT_EQ(FileSize(), static_cast<off_t>(kCapacity));
}

// A crash during an extension leaves a file whose size stopped partway,
// because the zeroes are what advances it. The next start reserves the
// rest.
TEST_F(WalFrameTest, AHalfWrittenCapacityIsCompleted) {
  AppendEpochs({1});
  const off_t log_end = EndOfLog();

  ASSERT_EQ(::truncate(wal_path().c_str(),
                       log_end + static_cast<off_t>(kCapacity) / 4),
            0);

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_FALSE(result.tail_truncated);
  EXPECT_EQ(result.frontier, 1u);
  EXPECT_EQ(wal.write_offset(), log_end);
  EXPECT_EQ(FileSize(), static_cast<off_t>(kCapacity));
}

// A reservation that fails partway leaves the file as far along as it got.
// The next start finishes it, and the log it already held is untouched.
TEST_F(WalFrameTest, AnInterruptedReservationIsCompletedOnTheNextStart) {
  // Written without preallocation, so that the file still ends at the log
  // and the reservation below has something to do.
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(),
            Wal::kNoPreallocation);
    ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
    std::map<EpochNumber, LogRecords> buckets;
    buckets[1] = MakeRecords(1, "k1");
    ASSERT_TRUE(wal.AppendGroup(buckets, 1).ok);
  }
  const off_t log_end = FileSize();

  {
    off_t allowed = static_cast<off_t>(kCapacity) / 4;
    LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
    io.initialise_pwrite = [&allowed](int fd, const void* data, size_t size,
                                      off_t offset) -> ssize_t {
      if (allowed <= 0) {
        errno = EIO;
        return -1;
      }
      const ssize_t written =
          ::pwrite(fd, data, std::min<size_t>(size, 4096), offset);
      if (written > 0) allowed -= written;
      return written;
    };

    Wal wal(work_dir_, io, kCapacity);
    const auto result = wal.ScanAndRepair();
    EXPECT_EQ(result.status, WalScanResult::Status::IoError);
    EXPECT_EQ(wal.write_offset(), 0);
  }
  ASSERT_GT(FileSize(), log_end);
  ASSERT_LT(FileSize(), static_cast<off_t>(kCapacity));

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_FALSE(result.tail_truncated);
  EXPECT_EQ(result.frontier, 1u);
  EXPECT_EQ(wal.write_offset(), log_end);
  EXPECT_EQ(FileSize(), static_cast<off_t>(kCapacity));
}

// A capacity of one byte reserves exactly what each group needs, which is
// the worst case for rounding the target up and the case that would expose
// a step per unit rather than a division.
TEST_F(WalFrameTest, ACapacityOfOneByteReservesPerGroup) {
  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), 1);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);

  for (EpochNumber epoch = 1; epoch <= 3; ++epoch) {
    std::map<EpochNumber, LogRecords> buckets;
    buckets[epoch] = MakeRecords(epoch, "k" + std::to_string(epoch));
    ASSERT_TRUE(wal.AppendGroup(buckets, epoch).ok);
    EXPECT_EQ(FileSize(), wal.write_offset());
  }
  EXPECT_EQ(wal.extension_count(), 3u);

  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_EQ(result.frontier, 3u);
  EXPECT_EQ(result.records.size(), 3u);
}

// A log that outgrows its capacity is extended rather than refused, and the
// extension is counted: it is a synchronous write of a whole new region,
// which a measurement of the flush alone has to see is absent.
TEST_F(WalFrameTest, OutgrowingCapacityExtendsAndIsCounted) {
  constexpr uint64_t kTinyCapacity = 4096;
  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kTinyCapacity);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  ASSERT_EQ(FileSize(), static_cast<off_t>(kTinyCapacity));

  for (EpochNumber epoch = 1; epoch <= 60; ++epoch) {
    std::map<EpochNumber, LogRecords> buckets;
    buckets[epoch] =
        MakeRecords(epoch, std::string(200, 'x') + std::to_string(epoch));
    ASSERT_TRUE(wal.AppendGroup(buckets, epoch).ok);
  }
  ASSERT_GT(wal.write_offset(), static_cast<off_t>(kTinyCapacity));
  EXPECT_GT(wal.extension_count(), 0u);
  EXPECT_GE(FileSize(), wal.write_offset());
  EXPECT_EQ(FileSize() % static_cast<off_t>(kTinyCapacity), 0);

  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_EQ(result.frontier, 60u);
  EXPECT_EQ(result.records.size(), 60u);
}

// Offsets are this process's to assign once the file is no longer opened for
// appending, so a second holder would write over frames rather than after
// them.
TEST_F(WalFrameTest, ASecondHolderIsRefused) {
  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  EXPECT_THROW(Wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity),
               std::system_error);
}

// A log written before capacity existed can be larger than the capacity a
// later start asks for. It is kept, not cut back to fit.
TEST_F(WalFrameTest, ALegacyLogLargerThanCapacityIsPreserved) {
  constexpr uint64_t kTinyCapacity = 4096;
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(),
            Wal::kNoPreallocation);
    ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
    for (EpochNumber epoch = 1; epoch <= 40; ++epoch) {
      std::map<EpochNumber, LogRecords> buckets;
      buckets[epoch] =
          MakeRecords(epoch, std::string(200, 'x') + std::to_string(epoch));
      ASSERT_TRUE(wal.AppendGroup(buckets, epoch).ok);
    }
    ASSERT_GT(wal.write_offset(), static_cast<off_t>(kTinyCapacity));
  }
  const off_t grown = FileSize();

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kTinyCapacity);
  const auto result = wal.ScanAndRepair();
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_EQ(result.frontier, 40u);
  EXPECT_EQ(result.records.size(), 40u);
  EXPECT_EQ(FileSize(), grown);
  EXPECT_EQ(wal.write_offset(), grown);
}

// Without preallocation the file tracks the log exactly, which is what a
// database that writes no record at all should leave behind.
TEST_F(WalFrameTest, WithoutPreallocationTheFileTracksTheLog) {
  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(),
          Wal::kNoPreallocation);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  EXPECT_EQ(FileSize(), 0);

  for (EpochNumber epoch = 1; epoch <= 3; ++epoch) {
    std::map<EpochNumber, LogRecords> buckets;
    buckets[epoch] = MakeRecords(epoch, "k" + std::to_string(epoch));
    ASSERT_TRUE(wal.AppendGroup(buckets, epoch).ok);
    EXPECT_EQ(FileSize(), wal.write_offset());
  }
  EXPECT_EQ(wal.extension_count(), 0u);
}

// Reserving capacity is a startup failure of its own, told apart from a
// group's failure by its own seam. Nothing may be published from a scan
// that hit one.
TEST_F(WalFrameTest, AFailureToReserveCapacityIsReported) {
  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  io.initialise_pwrite = [](int, const void*, size_t, off_t) -> ssize_t {
    errno = ENOSPC;
    return -1;
  };

  Wal wal(work_dir_, io, kCapacity);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::IoError);
  EXPECT_EQ(result.error_number, ENOSPC);
  EXPECT_EQ(wal.write_offset(), 0);
}

// A capacity that cannot be expressed as an offset is refused rather than
// turned into a write of that size.
TEST_F(WalFrameTest, ACapacityBeyondTheOffsetRangeIsRefused) {
  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), UINT64_MAX);
  const auto result = wal.ScanAndRepair();
  EXPECT_EQ(result.status, WalScanResult::Status::IoError);
  EXPECT_EQ(result.error_number, EFBIG);
  EXPECT_EQ(wal.write_offset(), 0);
  EXPECT_EQ(FileSize(), 0);
}

TEST_F(WalFrameTest, EmptyGroupNeitherWritesNorSyncs) {
  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  bool wrote = false;
  bool synced = false;
  io.pwrite = [&wrote](int fd, const void* data, size_t size, off_t offset) {
    wrote = true;
    return ::pwrite(fd, data, size, offset);
  };
  io.fdatasync = [&synced](int fd) {
    synced = true;
    return ::fdatasync(fd);
  };

  Wal wal(work_dir_, io, kCapacity);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[5] = MakeRecords(5, "k5");
  // Target below every bucket: nothing is eligible.
  const auto result = wal.AppendGroup(buckets, 4);
  EXPECT_TRUE(result.ok);
  EXPECT_FALSE(wrote);
  EXPECT_FALSE(synced);
  EXPECT_EQ(wal.write_offset(), 0);
}

// The hop's whole point: a covered frame costs one header read and nothing
// else, except the one frame right before the tail, which is read in full as
// a guard on the boundary the caller is trusting.
TEST_F(WalFrameTest, HopReadsOnlyTheGuardAndTailPayloads) {
  AppendEpochs({1, 2, 3, 4, 5});

  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  auto header_reads = std::make_shared<int>(0);
  auto payload_reads = std::make_shared<int>(0);
  auto real_pread = io.pread;
  // A frame's payload here is a few dozen bytes; the only reads anywhere near
  // capacity-sized are the unrelated end-of-log scan this test does not mean
  // to count.
  io.pread = [real_pread, header_reads, payload_reads](
                 int fd, void* data, size_t size, off_t offset) -> ssize_t {
    if (size == Wal::kHeaderSize) {
      ++*header_reads;
    } else if (size > Wal::kHeaderSize && size < 4096) {
      ++*payload_reads;
    }
    return real_pread(fd, data, size, offset);
  };

  WalScanResult hopped;
  {
    Wal wal(work_dir_, io, kCapacity);
    hopped = wal.ScanAndRepair(3);
  }
  ASSERT_EQ(hopped.status, WalScanResult::Status::Ok) << hopped.detail;
  EXPECT_EQ(hopped.frames_skipped, 3u);
  EXPECT_FALSE(hopped.tail_truncated);
  ASSERT_EQ(hopped.records.size(), 2u);
  EXPECT_EQ(hopped.records[0].epoch, 4u);
  EXPECT_EQ(hopped.records[1].epoch, 5u);

  // Header reads: 3 hopped frames, +1 to see the frame after them is past
  // min_epoch, +3 more as the resuming scan re-reads that frame, the tail,
  // and the all-zero header ending the log. 4 + 3 = 7.
  EXPECT_EQ(*header_reads, 7);
  // One payload read per frame that is not a pure hop: the guard at epoch 3
  // plus the two tail frames.
  EXPECT_EQ(*payload_reads, 3);

  Wal full_wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
  const auto full = full_wal.ScanAndRepair(0);
  ASSERT_EQ(full.status, WalScanResult::Status::Ok);
  EXPECT_EQ(hopped.frontier, full.frontier);
  // bytes_skipped covers exactly the three hopped frames: the offset one past
  // the third is where the first replayed frame, epoch 4, begins.
  const off_t third_frame_end = FrameEnd(FrameEnd(FrameEnd(0)));
  EXPECT_EQ(hopped.bytes_skipped, static_cast<uint64_t>(third_frame_end));
}

// The whole log at or below min_epoch: the guard is the true last frame, and
// end-of-log handling has to run exactly as it would without a hop.
TEST_F(WalFrameTest, HopOfTheWholeLogStillFinishesTheScan) {
  AppendEpochs({1, 2, 3});
  const off_t log_end = EndOfLog();

  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  auto payload_reads = std::make_shared<int>(0);
  auto real_pread = io.pread;
  // The end-of-log check reads the whole capacity looking for a surviving
  // frame once it finds the all-zero header past the last one; that read is
  // far larger than any frame's payload here and is not what this counts.
  io.pread = [real_pread, payload_reads](int fd, void* data, size_t size,
                                         off_t offset) -> ssize_t {
    if (size > Wal::kHeaderSize && size < 4096) ++*payload_reads;
    return real_pread(fd, data, size, offset);
  };

  Wal wal(work_dir_, io, kCapacity);
  const auto result = wal.ScanAndRepair(3);
  ASSERT_EQ(result.status, WalScanResult::Status::Ok) << result.detail;
  EXPECT_EQ(result.frontier, 3u);
  EXPECT_EQ(result.frames_skipped, 3u);
  EXPECT_TRUE(result.records.empty());
  EXPECT_FALSE(result.tail_truncated);
  EXPECT_EQ(wal.write_offset(), log_end);
  // Only the guard, the true last frame of the log, has its payload read.
  EXPECT_EQ(*payload_reads, 1);
}

// A corrupted length inside the covered region lands the hop's next header
// read on bytes that don't parse; it can't tell that apart from a lie only
// in the last covered frame, so it falls back to a full scan from offset 0.
TEST_F(WalFrameTest, ACorruptedLengthInTheCoveredRegionFallsBackAndStaysCorrect) {
  AppendEpochs({1, 2, 3, 4, 5});
  // Frame 1's declared length, shrunk so the hop's blind trust in it lands
  // mid-frame-1's own real payload rather than on frame 2's header.
  SetPayloadLengthAt(0, 4);

  WalScanResult hop_result;
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
    hop_result = wal.ScanAndRepair(4);
  }
  WalScanResult full_result;
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), kCapacity);
    full_result = wal.ScanAndRepair(0);
  }

  EXPECT_EQ(full_result.status, WalScanResult::Status::Corrupt);
  EXPECT_EQ(hop_result.status, full_result.status);
  EXPECT_EQ(hop_result.detail, full_result.detail);
  EXPECT_FALSE(hop_result.tail_truncated);
}

TEST_F(WalFrameTest, InjectedFdatasyncFailsAfterTheAllowedCalls) {
  ASSERT_EQ(::setenv("LINEAIRDB_WAL_FDATASYNC_FAIL_AFTER", "2", 1), 0);
  LineairDB::Recovery::WalIo io = LineairDB::Recovery::WalIo::Posix();
  // The factory captured the count; the variable must not leak to later
  // tests.
  ASSERT_EQ(::unsetenv("LINEAIRDB_WAL_FDATASYNC_FAIL_AFTER"), 0);

  Wal wal(work_dir_, io, kCapacity);
  ASSERT_EQ(wal.ScanAndRepair().status, WalScanResult::Status::Ok);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  ASSERT_TRUE(wal.AppendGroup(buckets, 1).ok);
  buckets.clear();
  buckets[2] = MakeRecords(2, "k2");
  ASSERT_TRUE(wal.AppendGroup(buckets, 2).ok);
  buckets.clear();
  buckets[3] = MakeRecords(3, "k3");
  const auto result = wal.AppendGroup(buckets, 3);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error_number, EIO);

  // The failed sync poisons the instance exactly like a failed write: a
  // later append is refused before it touches the file.
  const off_t offset_after_failure = wal.write_offset();
  buckets.clear();
  buckets[4] = MakeRecords(4, "k4");
  EXPECT_DEATH(wal.AppendGroup(buckets, 4), "");
  EXPECT_EQ(wal.write_offset(), offset_after_failure);
}

TEST_F(WalFrameTest, AnUnparsableInjectionCountStopsStartup) {
  const char* const malformed[] = {
      "",      // armed with nothing
      "abc",   // not a number
      "1junk",  // trailing garbage
      "-1",    // sign
      "+1",    // sign
      " 1",    // leading whitespace
      "99999999999999999999",  // out of long range
  };
  for (const char* value : malformed) {
    ASSERT_EQ(::setenv("LINEAIRDB_WAL_FDATASYNC_FAIL_AFTER", value, 1), 0);
    EXPECT_EXIT(LineairDB::Recovery::WalIo::Posix(),
                ::testing::ExitedWithCode(EXIT_FAILURE), "")
        << "value: " << value;
  }
  ASSERT_EQ(::unsetenv("LINEAIRDB_WAL_FDATASYNC_FAIL_AFTER"), 0);
}

}  // namespace
