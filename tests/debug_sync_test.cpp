#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include "util/debug_sync.hpp"

namespace {

constexpr auto kTestTimeout = std::chrono::seconds(5);

// Closes both ends on scope exit so an assertion failure cannot leak them.
class Pipe {
 public:
  Pipe() { EXPECT_EQ(::pipe(fds_), 0); }
  ~Pipe() {
    CloseRead();
    CloseWrite();
  }
  int read_fd() const { return fds_[0]; }
  int write_fd() const { return fds_[1]; }
  void CloseRead() { Close(&fds_[0]); }
  void CloseWrite() { Close(&fds_[1]); }

 private:
  static void Close(int* fd) {
    if (*fd >= 0) {
      ::close(*fd);
      *fd = -1;
    }
  }
  int fds_[2] = {-1, -1};
};

// Writes the release byte on scope exit. Declared after the future so it runs
// first and the blocked point can always finish; the extra byte on the
// success path is never read and harmless.
struct ReleaseOnExit {
  int fd;
  ~ReleaseOnExit() {
    [[maybe_unused]] const ssize_t rc = ::write(fd, "r", 1);
  }
};

// The facility decides once per process whether anything is armed. This
// sentinel keeps that decision armed in every execution order; without it, a
// test that arms nothing could run first and cache "nothing armed" for the
// rest of the file.
class DebugSyncTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    ::setenv("LINEAIRDB_DEBUG_SYNC_KEEPS_THE_FACILITY_ARMED", "sleep:0", 1);
  }

  void Arm(const std::string& variable, const std::string& action) {
    ::setenv(variable.c_str(), action.c_str(), 1);
    armed_.push_back(variable);
  }

  void TearDown() override {
    for (const auto& variable : armed_) {
      ::unsetenv(variable.c_str());
    }
    armed_.clear();
  }

 private:
  std::vector<std::string> armed_;
};

TEST_F(DebugSyncTest, ArriveAndWaitBlocksUntilReleased) {
  Pipe arrived;
  Pipe release;
  ASSERT_GE(arrived.write_fd(), 0);
  ASSERT_GE(release.read_fd(), 0);
  Arm("LINEAIRDB_DEBUG_SYNC_TEST_HANDSHAKE",
      "arrive_and_wait:" + std::to_string(arrived.write_fd()) + ":" +
          std::to_string(release.read_fd()));

  auto reached = std::async(std::launch::async, [] {
    LINEAIRDB_DEBUG_SYNC("test.handshake");
  });
  ReleaseOnExit always_release{release.write_fd()};

  char announcement = 0;
  ASSERT_EQ(::read(arrived.read_fd(), &announcement, 1), 1);
  // Arrival is announced before the point returns, which is the whole point:
  // the observer knows where the thread is and decides when it continues.
  EXPECT_EQ(reached.wait_for(std::chrono::milliseconds(200)),
            std::future_status::timeout);

  ASSERT_EQ(::write(release.write_fd(), "r", 1), 1);
  ASSERT_EQ(reached.wait_for(kTestTimeout), std::future_status::ready);
  reached.get();
}

TEST_F(DebugSyncTest, AnUnarmedPointFallsThrough) {
  // No variable for this name: the point must return without blocking. The
  // process is armed by the sentinel, so this exercises the lookup-miss path
  // of an armed process, not the cached fast path of an unarmed one.
  auto start = std::chrono::steady_clock::now();
  LINEAIRDB_DEBUG_SYNC("test.not_armed");
  EXPECT_LT(std::chrono::steady_clock::now() - start,
            std::chrono::milliseconds(50));
}

TEST_F(DebugSyncTest, SleepStillWorks) {
  Arm("LINEAIRDB_DEBUG_SYNC_TEST_SLEEP", "sleep:120");
  auto start = std::chrono::steady_clock::now();
  LINEAIRDB_DEBUG_SYNC("test.sleep");
  EXPECT_GE(std::chrono::steady_clock::now() - start,
            std::chrono::milliseconds(100));
}

TEST_F(DebugSyncTest, ClosedReleasePipeIsAFailure) {
  Pipe arrived;
  Pipe release;
  Arm("LINEAIRDB_DEBUG_SYNC_TEST_EOF",
      "arrive_and_wait:" + std::to_string(arrived.write_fd()) + ":" +
          std::to_string(release.read_fd()));
  release.CloseWrite();

  // Read of a pipe with no writer returns 0. Continuing would run the code
  // after the point as if the observer had released it.
  EXPECT_DEATH(LINEAIRDB_DEBUG_SYNC("test.eof"), "was never released");
}

TEST_F(DebugSyncTest, BrokenArrivalPipeIsAFailure) {
  Pipe arrived;
  Pipe release;
  Arm("LINEAIRDB_DEBUG_SYNC_TEST_NO_READER",
      "arrive_and_wait:" + std::to_string(arrived.write_fd()) + ":" +
          std::to_string(release.read_fd()));
  arrived.CloseRead();

  // With no reader, write raises SIGPIPE; the point must still die through
  // its own diagnostic rather than the signal's default action.
  EXPECT_DEATH(LINEAIRDB_DEBUG_SYNC("test.no_reader"),
               "could not announce arrival");
}

TEST_F(DebugSyncTest, AnUnusableDescriptorIsAFailure) {
  // The descriptors must not exist in this process, or the death would be
  // about something other than announcement failure.
  ASSERT_EQ(::fcntl(987654, F_GETFD), -1);
  ASSERT_EQ(::fcntl(987655, F_GETFD), -1);
  Arm("LINEAIRDB_DEBUG_SYNC_TEST_BAD_FD", "arrive_and_wait:987654:987655");
  EXPECT_DEATH(LINEAIRDB_DEBUG_SYNC("test.bad_fd"),
               "could not announce arrival");
}

TEST_F(DebugSyncTest, MalformedActivationsAreFailures) {
  const std::string variable = "LINEAIRDB_DEBUG_SYNC_TEST_MALFORMED";
  const char* const malformed[] = {
      "arrive_and_wait:",          // no descriptors
      "arrive_and_wait:3",         // only one
      "arrive_and_wait:3:4:5",     // one too many
      "arrive_and_wait:-1:4",      // negative
      "arrive_and_wait:3:-4",      // negative in the second position
      "arrive_and_wait: 3:4",      // leading whitespace
      "arrive_and_wait:+3:4",      // explicit sign
      "arrive_and_wait:-0:4",      // negative zero
      "arrive_and_wait:3:+4",      // sign in the second position
      "arrive_and_wait:x:4",       // not a number
      "arrive_and_wait:99999999999999999999:4",  // out of range
      "arrive_and_wait:3:99999999999999999999",
      // Above INT_MAX but within long on LP64, so strtol reports no error and
      // only the explicit bound rejects it.
      "arrive_and_wait:2147483648:4",
      "arrive_and_wait:3:2147483648",
      "sleep:",                    // no duration
      "sleep:abc",                 // not a number
      "sleep:1junk",               // trailing garbage
      "sleep:-5",                  // sign
      "sleep: 5",                  // leading whitespace
      "sleep:99999999999999999999",  // out of long range
      "hold_forever",              // an action that does not exist
      "",                          // armed with nothing
  };
  for (const char* action : malformed) {
    Arm(variable, action);
    EXPECT_DEATH(LINEAIRDB_DEBUG_SYNC("test.malformed"),
                 "LineairDB debug sync point")
        << "action: " << action;
  }
}

}  // namespace
