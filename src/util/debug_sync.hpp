#ifndef LINEAIRDB_UTIL_DEBUG_SYNC_HPP
#define LINEAIRDB_UTIL_DEBUG_SYNC_HPP

// Debug Sync facility, after MySQL's DEBUG_SYNC (sql/debug_sync.h):
// production code marks a named synchronization point with one macro line,
// and the point's behavior is injected from outside the binary. MySQL
// compiles its points out of release builds; here the points stay compiled
// in and are gated at runtime instead, so the exact binary under test is
// the one that serves production traffic. A process with no
// LINEAIRDB_DEBUG_SYNC_* environment variables evaluates each point as a
// call into the cached enabled check plus one branch.
//
// Marking a point:
//   LINEAIRDB_DEBUG_SYNC("stateless_commit.between_row_installs");
//
// Activating a point (environment):
//   LINEAIRDB_DEBUG_SYNC_STATELESS_COMMIT_BETWEEN_ROW_INSTALLS=sleep:1500
// The variable name is the point name upper-cased with '.' mapped to '_'.
// The prefix scan that answers "is anything armed" is cached at first use;
// an armed process re-reads the point's action on every hit.
// Supported actions: sleep:<ms>, clamped to [0, 10000].

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

extern char** environ;

namespace LineairDB {
namespace Util {

inline bool DebugSyncEnabled() {
  static const bool enabled = [] {
    constexpr char kPrefix[] = "LINEAIRDB_DEBUG_SYNC_";
    for (char** e = environ; *e != nullptr; ++e) {
      if (std::strncmp(*e, kPrefix, sizeof(kPrefix) - 1) == 0) return true;
    }
    return false;
  }();
  return enabled;
}

// Slow path: runs only when at least one point is activated.
inline void DebugSyncPoint(const char* point_name) {
  std::string var = "LINEAIRDB_DEBUG_SYNC_";
  for (const char* p = point_name; *p != '\0'; ++p) {
    var.push_back(*p == '.' ? '_'
                            : static_cast<char>(std::toupper(
                                  static_cast<unsigned char>(*p))));
  }
  const char* action = std::getenv(var.c_str());
  if (action == nullptr) return;
  constexpr char kSleep[] = "sleep:";
  if (std::strncmp(action, kSleep, sizeof(kSleep) - 1) == 0) {
    const long ms = std::clamp(
        std::strtol(action + sizeof(kSleep) - 1, nullptr, 10), 0L, 10000L);
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
  }
}

}  // namespace Util
}  // namespace LineairDB

#define LINEAIRDB_DEBUG_SYNC(point_name)                 \
  do {                                                   \
    if (::LineairDB::Util::DebugSyncEnabled()) {         \
      ::LineairDB::Util::DebugSyncPoint(point_name);     \
    }                                                    \
  } while (0)

#endif  // LINEAIRDB_UTIL_DEBUG_SYNC_HPP
