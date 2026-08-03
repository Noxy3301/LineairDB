#include "crc32c.h"

#include <array>

namespace LineairDB {
namespace Recovery {

namespace {

constexpr uint32_t kReflectedPolynomial = 0x82f63b78u;

constexpr std::array<uint32_t, 256> MakeTable() {
  std::array<uint32_t, 256> table{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t entry = i;
    for (int bit = 0; bit < 8; ++bit) {
      entry = (entry >> 1) ^ ((entry & 1u) ? kReflectedPolynomial : 0u);
    }
    table[i] = entry;
  }
  return table;
}

constexpr std::array<uint32_t, 256> kTable = MakeTable();

}  // namespace

void Crc32c::Update(const void* data, size_t size) {
  const auto* bytes = static_cast<const uint8_t*>(data);
  for (size_t i = 0; i < size; ++i) {
    state_ = kTable[(state_ ^ bytes[i]) & 0xffu] ^ (state_ >> 8);
  }
}

uint32_t ComputeCrc32c(const void* data, size_t size) {
  Crc32c crc;
  crc.Update(data, size);
  return crc.Finish();
}

}  // namespace Recovery
}  // namespace LineairDB
