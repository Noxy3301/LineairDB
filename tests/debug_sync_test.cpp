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

// The facility decides once per process whether anything is armed, so one
// variable stays set for every test here. Without it the first point evaluated
// would cache "nothing armed" and the rest of the file would test nothing.
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
  int arrived[2];
  int release[2];
  ASSERT_EQ(::pipe(arrived), 0);
  ASSERT_EQ(::pipe(release), 0);
  Arm("LINEAIRDB_DEBUG_SYNC_TEST_HANDSHAKE",
      "arrive_and_wait:" + std::to_string(arrived[1]) + ":" +
          std::to_string(release[0]));

  auto reached = std::async(std::launch::async, [] {
    LINEAIRDB_DEBUG_SYNC("test.handshake");
  });

  char announcement = 0;
  ASSERT_EQ(::read(arrived[0], &announcement, 1), 1);
  // Arrival is announced before the point returns, which is the whole point:
  // the observer knows where the thread is and decides when it continues.
  EXPECT_EQ(reached.wait_for(std::chrono::milliseconds(200)),
            std::future_status::timeout);

  ASSERT_EQ(::write(release[1], "r", 1), 1);
  ASSERT_EQ(reached.wait_for(kTestTimeout), std::future_status::ready);
  reached.get();

  for (int fd : {arrived[0], arrived[1], release[0], release[1]}) {
    ::close(fd);
  }
}

TEST_F(DebugSyncTest, AnUnarmedPointCostsNothing) {
  // No variable for this name: the point must fall through.
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
  int arrived[2];
  int release[2];
  ASSERT_EQ(::pipe(arrived), 0);
  ASSERT_EQ(::pipe(release), 0);
  Arm("LINEAIRDB_DEBUG_SYNC_TEST_EOF",
      "arrive_and_wait:" + std::to_string(arrived[1]) + ":" +
          std::to_string(release[0]));
  ::close(release[1]);

  // Read of a pipe with no writer returns 0. Continuing would run the code
  // after the point as if the observer had released it.
  EXPECT_DEATH(LINEAIRDB_DEBUG_SYNC("test.eof"), "was never released");

  for (int fd : {arrived[0], arrived[1], release[0]}) {
    ::close(fd);
  }
}

TEST_F(DebugSyncTest, AnUnusableDescriptorIsAFailure) {
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
      "arrive_and_wait:x:4",       // not a number
      "arrive_and_wait:99999999999999999999:4",  // out of range
      "arrive_and_wait:3:99999999999999999999",
      // Above INT_MAX but within long on LP64, so strtol reports no error and
      // only the explicit bound rejects it.
      "arrive_and_wait:2147483648:4",
      "arrive_and_wait:3:2147483648",
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
