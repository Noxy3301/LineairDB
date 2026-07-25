#ifndef LINEAIRDB_RECOVERY_CRC32C_H
#define LINEAIRDB_RECOVERY_CRC32C_H

#include <cstddef>
#include <cstdint>

namespace LineairDB {
namespace Recovery {

/**
 * CRC-32C (Castagnoli): reflected polynomial 0x82F63B78, initial value
 * 0xFFFFFFFF, final xor 0xFFFFFFFF. The check value for "123456789" is
 * 0xE3069283.
 *
 * The polynomial is pinned because "crc32" names two different algorithms:
 * zlib's crc32() computes IEEE CRC-32 and will not validate these frames.
 */
class Crc32c {
 public:
  void Update(const void* data, size_t size);
  uint32_t Finish() const { return state_ ^ 0xffffffffu; }

 private:
  uint32_t state_{0xffffffffu};
};

uint32_t ComputeCrc32c(const void* data, size_t size);

}  // namespace Recovery
}  // namespace LineairDB

#endif /* LINEAIRDB_RECOVERY_CRC32C_H */
