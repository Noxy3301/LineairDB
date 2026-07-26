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
//
// Supported actions:
//   sleep:<ms>
//     Sleeps, clamped to [0, 10000]. Orders nothing: it makes a window wider,
//     which is enough to provoke a race but never enough to prove an order.
//   arrive_and_wait:<arrived_write_fd>:<release_read_fd>
//     Writes one byte to the first descriptor and blocks until one byte can be
//     read from the second. The observer therefore knows the process is inside
//     the point, and the process stays there until the observer says otherwise,
//     which is what an ordering assertion needs. The descriptors are inherited
//     from the process that started this one (Python: os.pipe() plus Popen's
//     pass_fds). A descriptor that cannot be used, or an action that does not
//     parse, is a broken test rather than a production condition, and stops the
//     process instead of continuing unsynchronized.

#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdio>
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

[[noreturn]] inline void DebugSyncFatal(const char* point_name,
                                        const char* detail) {
  std::fprintf(stderr, "LineairDB debug sync point '%s': %s\n", point_name,
               detail);
  std::abort();
}

// Reads one descriptor from *cursor and leaves it past the separator. A value
// that is not a descriptor at all -- out of int range, negative, or followed by
// the wrong character -- is a broken activation, not an unlucky one.
inline int DebugSyncParseDescriptor(const char* point_name, const char** cursor,
                                   char separator) {
  errno = 0;
  char* end = nullptr;
  const long value = std::strtol(*cursor, &end, 10);
  if (end == *cursor || *end != separator || errno == ERANGE || value < 0 ||
      value > INT_MAX) {
    DebugSyncFatal(point_name, "expected arrive_and_wait:<fd>:<fd>");
  }
  *cursor = (separator == '\0') ? end : end + 1;
  return static_cast<int>(value);
}

// Announces arrival on one descriptor and blocks on the other. Short I/O and
// EOF are failures: the point would otherwise continue as if released.
inline void DebugSyncArriveAndWait(const char* point_name, int arrived_fd,
                                   int release_fd) {
  const char arrived = 'a';
  ssize_t written = 0;
  do {
    written = ::write(arrived_fd, &arrived, 1);
  } while (written < 0 && errno == EINTR);
  if (written != 1) {
    DebugSyncFatal(point_name, "could not announce arrival");
  }

  char release = 0;
  ssize_t received = 0;
  do {
    received = ::read(release_fd, &release, 1);
  } while (received < 0 && errno == EINTR);
  if (received != 1) {
    DebugSyncFatal(point_name, "was never released");
  }
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
    return;
  }

  constexpr char kArriveAndWait[] = "arrive_and_wait:";
  if (std::strncmp(action, kArriveAndWait, sizeof(kArriveAndWait) - 1) == 0) {
    const char* cursor = action + sizeof(kArriveAndWait) - 1;
    const int arrived_fd = DebugSyncParseDescriptor(point_name, &cursor, ':');
    const int release_fd = DebugSyncParseDescriptor(point_name, &cursor, '\0');
    DebugSyncArriveAndWait(point_name, arrived_fd, release_fd);
    return;
  }

  DebugSyncFatal(point_name, "unknown action");
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
