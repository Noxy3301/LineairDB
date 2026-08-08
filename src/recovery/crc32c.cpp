#include "crc32c.h"

#include <array>
#include <cstring>

#if defined(__x86_64__)
#include <nmmintrin.h>
#define LINEAIRDB_CRC32C_X86_SSE42 1
#endif

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

uint32_t UpdateWithTable(uint32_t state, const uint8_t* bytes, size_t size) {
  for (size_t i = 0; i < size; ++i) {
    state = kTable[(state ^ bytes[i]) & 0xffu] ^ (state >> 8);
  }
  return state;
}

#if LINEAIRDB_CRC32C_X86_SSE42

// Compiled via the target attribute, not -msse4.2/-march=native, so it builds
// in every configuration; HasSse42() is what keeps it off CPUs without it.
__attribute__((target("sse4.2"))) uint32_t UpdateWithSse42(
    uint32_t state, const uint8_t* bytes, size_t size) {
  uint64_t crc = state;
  while (size >= sizeof(uint64_t)) {
    uint64_t chunk;
    std::memcpy(&chunk, bytes, sizeof(chunk));
    crc = _mm_crc32_u64(crc, chunk);
    bytes += sizeof(chunk);
    size -= sizeof(chunk);
  }
  while (size > 0) {
    crc = _mm_crc32_u8(static_cast<uint32_t>(crc), *bytes);
    ++bytes;
    --size;
  }
  return static_cast<uint32_t>(crc);
}

bool HasSse42() {
  static const bool has_sse42 = __builtin_cpu_supports("sse4.2");
  return has_sse42;
}

#endif  // LINEAIRDB_CRC32C_X86_SSE42

}  // namespace

void Crc32c::Update(const void* data, size_t size) {
  const auto* bytes = static_cast<const uint8_t*>(data);
#if LINEAIRDB_CRC32C_X86_SSE42
  if (HasSse42()) {
    state_ = UpdateWithSse42(state_, bytes, size);
    return;
  }
#endif
  state_ = UpdateWithTable(state_, bytes, size);
}

uint32_t ComputeCrc32c(const void* data, size_t size) {
  Crc32c crc;
  crc.Update(data, size);
  return crc.Finish();
}

uint32_t UpdateWithTableForTesting(uint32_t state, const void* data,
                                   size_t size) {
  return UpdateWithTable(state, static_cast<const uint8_t*>(data), size);
}

uint32_t UpdateWithSse42ForTesting(uint32_t state, const void* data,
                                   size_t size) {
#if LINEAIRDB_CRC32C_X86_SSE42
  if (HasSse42()) {
    return UpdateWithSse42(state, static_cast<const uint8_t*>(data), size);
  }
#endif
  return UpdateWithTable(state, static_cast<const uint8_t*>(data), size);
}

bool HasSse42ForTesting() {
#if LINEAIRDB_CRC32C_X86_SSE42
  return HasSse42();
#else
  return false;
#endif
}

}  // namespace Recovery
}  // namespace LineairDB
