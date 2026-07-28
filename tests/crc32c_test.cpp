#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <random>
#include <vector>

#include "recovery/crc32c.h"

namespace {

using LineairDB::Recovery::ComputeCrc32c;
using LineairDB::Recovery::Crc32c;
using LineairDB::Recovery::HasSse42ForTesting;
using LineairDB::Recovery::UpdateWithSse42ForTesting;
using LineairDB::Recovery::UpdateWithTableForTesting;

constexpr uint32_t kInitialState = 0xffffffffu;

TEST(Crc32cTest, KnownVector) {
  EXPECT_EQ(ComputeCrc32c("123456789", 9), 0xE3069283u);
}

TEST(Crc32cTest, EmptyInput) {
  EXPECT_EQ(ComputeCrc32c("", 0), 0x00000000u);
  Crc32c crc;
  crc.Update(nullptr, 0);
  EXPECT_EQ(crc.Finish(), 0x00000000u);
}

TEST(Crc32cTest, ChunkedUpdateMatchesOneShot) {
  std::mt19937 rng(12345);
  std::uniform_int_distribution<int> byte_dist(0, 255);
  std::vector<uint8_t> data(4096 * 3 + 17);
  for (auto& b : data) b = static_cast<uint8_t>(byte_dist(rng));

  Crc32c one_shot;
  one_shot.Update(data.data(), data.size());

  // Chunk sizes crossing and misaligned with the hardware path's 8-byte
  // stride, so the seam between calls is exercised on both code paths.
  const size_t chunk_sizes[] = {1, 3, 8, 251, 4096};
  Crc32c chunked;
  size_t offset = 0;
  size_t chunk_index = 0;
  while (offset < data.size()) {
    const size_t chunk =
        std::min(chunk_sizes[chunk_index % 5], data.size() - offset);
    chunked.Update(data.data() + offset, chunk);
    offset += chunk;
    ++chunk_index;
  }

  EXPECT_EQ(chunked.Finish(), one_shot.Finish());
}

TEST(Crc32cTest, TableAndHardwarePathsAgreeOnRandomBuffers) {
  if (!HasSse42ForTesting()) {
    GTEST_SKIP() << "SSE4.2 not available on this CPU/build";
  }

  std::mt19937 rng(67890);
  std::uniform_int_distribution<int> byte_dist(0, 255);
  std::vector<size_t> sizes;
  // Every remainder mod the hardware path's 8-byte stride, up through a few
  // strides, plus larger sizes further from that boundary.
  for (size_t size = 0; size <= 23; ++size) sizes.push_back(size);
  for (const size_t size : {63u, 64u, 65u, 4096u, 100003u}) {
    sizes.push_back(size);
  }
  for (const size_t size : sizes) {
    std::vector<uint8_t> data(size);
    for (auto& b : data) b = static_cast<uint8_t>(byte_dist(rng));

    const uint32_t table_result =
        UpdateWithTableForTesting(kInitialState, data.data(), data.size());
    const uint32_t sse42_result =
        UpdateWithSse42ForTesting(kInitialState, data.data(), data.size());
    EXPECT_EQ(table_result, sse42_result) << "size=" << size;
  }
}

}  // namespace
