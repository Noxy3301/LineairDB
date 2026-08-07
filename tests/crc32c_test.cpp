#include "recovery/crc32c.h"

#include <cstdint>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace {

using LineairDB::Recovery::ComputeCrc32c;
using LineairDB::Recovery::Crc32c;

// Known-answer vectors from RFC 3720 appendix B.4, plus the conventional
// check value for "123456789". An IEEE CRC-32 (zlib) implementation fails
// every nonempty vector.

TEST(Crc32cTest, TheCheckValueMatches) {
  const std::string data = "123456789";
  EXPECT_EQ(0xe3069283u, ComputeCrc32c(data.data(), data.size()));
}

TEST(Crc32cTest, ThirtyTwoZeroBytesMatchRfc3720) {
  const std::vector<uint8_t> data(32, 0x00);
  EXPECT_EQ(0x8a9136aau, ComputeCrc32c(data.data(), data.size()));
}

TEST(Crc32cTest, ThirtyTwoFfBytesMatchRfc3720) {
  const std::vector<uint8_t> data(32, 0xff);
  EXPECT_EQ(0x62a8ab43u, ComputeCrc32c(data.data(), data.size()));
}

TEST(Crc32cTest, AscendingBytesMatchRfc3720) {
  std::vector<uint8_t> data;
  for (uint8_t i = 0; i < 32; ++i) data.push_back(i);
  EXPECT_EQ(0x46dd794eu, ComputeCrc32c(data.data(), data.size()));
}

TEST(Crc32cTest, DescendingBytesMatchRfc3720) {
  std::vector<uint8_t> data;
  for (int i = 31; 0 <= i; --i) data.push_back(static_cast<uint8_t>(i));
  EXPECT_EQ(0x113fdb5cu, ComputeCrc32c(data.data(), data.size()));
}

TEST(Crc32cTest, EmptyInputYieldsZero) {
  EXPECT_EQ(0x00000000u, ComputeCrc32c(nullptr, 0));
}

TEST(Crc32cTest, SplitUpdatesEqualOneShot) {
  const std::string data = "the quick brown fox jumps over the lazy dog";
  for (size_t split = 0; split <= data.size(); ++split) {
    Crc32c crc;
    crc.Update(data.data(), split);
    crc.Update(data.data() + split, data.size() - split);
    EXPECT_EQ(ComputeCrc32c(data.data(), data.size()), crc.Finish());
  }
}

}  // namespace
