#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>
#include <sys/select.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "database_impl.h"
#include "gtest/gtest.h"
#include "test_helper.hpp"

namespace {

using LineairDB::EpochNumber;

constexpr auto kBarrierTimeout = std::chrono::seconds(10);
constexpr auto kArrivalTimeout = std::chrono::milliseconds(10000);
// Longer than the two epochs the barrier needs before it can wait on the log,
// so a barrier that is not waiting has certainly returned by the time it
// elapses.
constexpr auto kNotAnsweredFor = std::chrono::milliseconds(500);
// Everything a timed call is allowed to overshoot by: thread start, the
// wake from the mutex, and one epoch tick.
constexpr auto kSchedulingSlack = std::chrono::milliseconds(200);
const char* const kWorkDir = "lineairdb_durability_barrier_logs";
const char* const kWalPoint = "LINEAIRDB_DEBUG_SYNC_WAL_BEFORE_FDATASYNC";
const char* const kBarrierPoint =
    "LINEAIRDB_DEBUG_SYNC_DATABASE_BEFORE_DURABILITY_BARRIER";
const char* const kCommitPoint =
    "LINEAIRDB_DEBUG_SYNC_DATABASE_END_TRANSACTION_BEFORE_OFFLINE";

/**
 * @brief Stops whoever reaches a named debug sync point, so the test decides
 * when they go on.
 *
 * @details The point is armed through the environment of this process; the
 * descriptors it hands out are this object's pipes.
 * @note Disarm() must run before the database is destroyed: shutdown drains
 * the flusher and would otherwise block on a parked group.
 */
class PointHold {
 public:
  explicit PointHold(const char* variable) : variable_(variable) {
    if (::pipe(arrived_) != 0 || ::pipe(release_) != 0) std::abort();
    const std::string action = "arrive_and_wait:" +
                               std::to_string(arrived_[1]) + ":" +
                               std::to_string(release_[0]);
    ::setenv(variable_, action.c_str(), 1);
  }

  ~PointHold() {
    Disarm();
    for (const int descriptor :
         {arrived_[0], arrived_[1], release_[0], release_[1]}) {
      ::close(descriptor);
    }
  }

  /** Stops further parking, then wakes whoever is parked or about to be. */
  void Disarm() {
    ::unsetenv(variable_);
    for (int attempt = 0; attempt < 16; attempt++) Release();
  }

  bool WaitForArrival(std::chrono::milliseconds timeout) {
    fd_set readable;
    FD_ZERO(&readable);
    FD_SET(arrived_[0], &readable);
    timeval limit{static_cast<time_t>(timeout.count() / 1000),
                  static_cast<suseconds_t>((timeout.count() % 1000) * 1000)};
    if (::select(arrived_[0] + 1, &readable, nullptr, nullptr, &limit) <= 0) {
      return false;
    }
    char token = 0;
    return ::read(arrived_[0], &token, 1) == 1;
  }

  void Release() {
    const char token = 'r';
    [[maybe_unused]] const ssize_t written = ::write(release_[1], &token, 1);
  }

  /** Answers every parked group, one release per arrival, until none comes. */
  void DrainUntilIdle() {
    while (WaitForArrival(std::chrono::milliseconds(200))) Release();
  }

 private:
  const char* variable_;
  int arrived_[2]{};
  int release_[2]{};
};

/** The configuration both tests run, matching the server's. */
LineairDB::Config BarrierConfig() {
  LineairDB::Config config;
  config.work_dir = kWorkDir;
  config.max_thread = 4;
  config.epoch_duration_ms = 40;
  config.concurrency_control_protocol =
      LineairDB::Config::ConcurrencyControl::Silo;
  config.index_structure = LineairDB::Config::IndexStructure::Masstree;
  config.commit_durability = LineairDB::Config::CommitDurability::Async;
  config.enable_recovery = false;
  config.enable_checkpointing = false;
  return config;
}

}  // namespace

/**
 * A commit acknowledged under Async is only in memory. The switch to Sync
 * must not return until that commit's own fdatasync has completed, because
 * from its return the caller is entitled to treat every earlier
 * acknowledgement as durable.
 *
 * The test below arms the point before this process constructs its first
 * Database, which is what makes the "is any point armed" check — cached at
 * its first hit — true for every test in this binary.
 */
TEST(DurabilityBarrierTest, SwitchWaitsForAnAsyncCommitToReachTheDevice) {
  std::filesystem::remove_all(kWorkDir);
  PointHold hold(kWalPoint);

  auto db = std::make_unique<LineairDB::Database>(BarrierConfig());
  // Declared after the database so unwinding frees the flusher first; an
  // assertion failure would otherwise hang in ~Database.
  struct DisarmOnExit {
    PointHold& hold;
    ~DisarmOnExit() { hold.Disarm(); }
  } disarm_on_exit{hold};

  auto* impl = LineairDB::Database::Impl::CurrentDBInstance;
  ASSERT_NE(impl, nullptr);
  ASSERT_EQ(db->GetCommitDurability(),
            LineairDB::Config::CommitDurability::Async);

  ASSERT_TRUE(TestHelper::DoTransactions(
      db.get(), {[](LineairDB::Transaction& tx) {
        tx.Write<int>("alice", 1);
      }}));
  db->Fence();

  // At or above the commit's epoch: the global epoch never decreases, and the
  // commit took its epoch while it was online.
  const EpochNumber commit_covered_by = impl->GetGlobalEpoch();

  // The only group with records is this commit's, so the flusher parks in it.
  ASSERT_TRUE(hold.WaitForArrival(kArrivalTimeout));
  ASSERT_LT(impl->GetDurableEpoch(), commit_covered_by);

  auto switched = std::async(std::launch::async, [&db] {
    return db->SetCommitDurability(LineairDB::Config::CommitDurability::Sync,
                                   kBarrierTimeout);
  });
  EXPECT_EQ(switched.wait_for(kNotAnsweredFor), std::future_status::timeout);
  EXPECT_LT(impl->GetDurableEpoch(), commit_covered_by);

  // Groups that close later park too; keep releasing until the barrier ends.
  hold.Release();
  const auto deadline = std::chrono::steady_clock::now() + kArrivalTimeout;
  while (switched.wait_for(std::chrono::milliseconds(50)) !=
             std::future_status::ready &&
         std::chrono::steady_clock::now() < deadline) {
    if (hold.WaitForArrival(std::chrono::milliseconds(50))) hold.Release();
  }
  ASSERT_EQ(switched.wait_for(std::chrono::seconds(0)),
            std::future_status::ready);
  EXPECT_TRUE(switched.get());

  // Read before any further commit and before the clean shutdown: the
  // frontier the barrier returned on already covers the commit.
  EXPECT_GE(impl->GetDurableEpoch(), commit_covered_by);
  EXPECT_EQ(db->GetCommitDurability(),
            LineairDB::Config::CommitDurability::Sync);

  // Disarmed only here: Disarm() releases without an arrival to answer, and a
  // spare token would wave the next group through a point the test still holds.
  hold.DrainUntilIdle();
  hold.Disarm();
  db.reset(nullptr);
  std::filesystem::remove_all(kWorkDir);
}

// A switch that queued behind another must expire on its own timeout, not
// when the one ahead of it finishes: its caller has a deadline, and so does
// the client waiting on the other end of it.
TEST(DurabilityBarrierTest, AQueuedSwitchExpiresOnItsOwnTimeout) {
  constexpr auto kQueuedTimeout = std::chrono::milliseconds(300);
  std::filesystem::remove_all(kWorkDir);
  PointHold hold(kWalPoint);

  auto db = std::make_unique<LineairDB::Database>(BarrierConfig());
  struct DisarmOnExit {
    PointHold& hold;
    ~DisarmOnExit() { hold.Disarm(); }
  } disarm_on_exit{hold};

  ASSERT_TRUE(TestHelper::DoTransactions(
      db.get(), {[](LineairDB::Transaction& tx) {
        tx.Write<int>("alice", 1);
      }}));
  db->Fence();
  ASSERT_TRUE(hold.WaitForArrival(kArrivalTimeout));

  auto first = std::async(std::launch::async, [&db] {
    return db->SetCommitDurability(LineairDB::Config::CommitDurability::Sync,
                                   kBarrierTimeout);
  });

  // The policy is published under the lock, so seeing Sync means the first
  // switch is inside the barrier and holding it.
  const auto published_by = std::chrono::steady_clock::now() + kArrivalTimeout;
  while (db->GetCommitDurability() !=
             LineairDB::Config::CommitDurability::Sync &&
         std::chrono::steady_clock::now() < published_by) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_EQ(db->GetCommitDurability(),
            LineairDB::Config::CommitDurability::Sync);
  ASSERT_EQ(first.wait_for(kNotAnsweredFor), std::future_status::timeout);

  const auto started = std::chrono::steady_clock::now();
  const bool queued = db->SetCommitDurability(
      LineairDB::Config::CommitDurability::Sync, kQueuedTimeout);
  const auto waited = std::chrono::steady_clock::now() - started;
  EXPECT_FALSE(queued);
  EXPECT_LT(waited, kQueuedTimeout + kSchedulingSlack);
  // Still held by the first switch, which the queued one did not wait out.
  EXPECT_EQ(first.wait_for(std::chrono::seconds(0)),
            std::future_status::timeout);

  hold.Release();
  const auto deadline = std::chrono::steady_clock::now() + kArrivalTimeout;
  while (first.wait_for(std::chrono::milliseconds(50)) !=
             std::future_status::ready &&
         std::chrono::steady_clock::now() < deadline) {
    if (hold.WaitForArrival(std::chrono::milliseconds(50))) hold.Release();
  }
  ASSERT_EQ(first.wait_for(std::chrono::seconds(0)),
            std::future_status::ready);
  EXPECT_TRUE(first.get());

  hold.DrainUntilIdle();
  hold.Disarm();
  db.reset(nullptr);
  std::filesystem::remove_all(kWorkDir);
}

// A switch that acquires late must still finish on what is left of its own
// budget, not be failed for having queued.
TEST(DurabilityBarrierTest, AQueuedSwitchRunsOnItsRemainingBudget) {
  constexpr auto kQueuedTimeout = std::chrono::milliseconds(600);
  std::filesystem::remove_all(kWorkDir);
  PointHold hold(kWalPoint);

  auto db = std::make_unique<LineairDB::Database>(BarrierConfig());
  struct DisarmOnExit {
    PointHold& hold;
    ~DisarmOnExit() { hold.Disarm(); }
  } disarm_on_exit{hold};

  ASSERT_TRUE(TestHelper::DoTransactions(
      db.get(), {[](LineairDB::Transaction& tx) {
        tx.Write<int>("alice", 1);
      }}));
  db->Fence();
  ASSERT_TRUE(hold.WaitForArrival(kArrivalTimeout));

  auto first = std::async(std::launch::async, [&db] {
    return db->SetCommitDurability(LineairDB::Config::CommitDurability::Sync,
                                   kBarrierTimeout);
  });
  const auto published_by = std::chrono::steady_clock::now() + kArrivalTimeout;
  while (db->GetCommitDurability() !=
             LineairDB::Config::CommitDurability::Sync &&
         std::chrono::steady_clock::now() < published_by) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_EQ(first.wait_for(std::chrono::seconds(0)),
            std::future_status::timeout);

  const auto started = std::chrono::steady_clock::now();
  auto queued = std::async(std::launch::async, [&db, kQueuedTimeout] {
    return db->SetCommitDurability(LineairDB::Config::CommitDurability::Sync,
                                   kQueuedTimeout);
  });

  // Let the first switch through halfway into the second one's budget, so it
  // acquires with about half of it left.
  std::this_thread::sleep_for(kQueuedTimeout / 2);
  hold.Disarm();

  ASSERT_EQ(queued.wait_for(kArrivalTimeout), std::future_status::ready);
  const auto waited = std::chrono::steady_clock::now() - started;
  EXPECT_TRUE(queued.get());
  EXPECT_LT(waited, kQueuedTimeout + kSchedulingSlack);
  ASSERT_EQ(first.wait_for(kArrivalTimeout), std::future_status::ready);
  EXPECT_TRUE(first.get());

  db.reset(nullptr);
  std::filesystem::remove_all(kWorkDir);
}

// The sharpest case: a commit parked between its policy capture and leaving
// its epoch holds the epoch clock, so a queued switch cannot make progress
// once it acquires. It must still end within its own budget rather than start
// a fresh one, which is what the old code did.
TEST(DurabilityBarrierTest, AQueuedSwitchDoesNotRestartItsBudget) {
  constexpr auto kQueuedTimeout = std::chrono::milliseconds(600);
  std::filesystem::remove_all(kWorkDir);
  PointHold flusher(kWalPoint);

  auto db = std::make_unique<LineairDB::Database>(BarrierConfig());
  struct DisarmOnExit {
    PointHold& hold;
    ~DisarmOnExit() { hold.Disarm(); }
  } disarm_flusher{flusher};

  ASSERT_TRUE(TestHelper::DoTransactions(
      db.get(), {[](LineairDB::Transaction& tx) {
        tx.Write<int>("alice", 1);
      }}));
  db->Fence();
  ASSERT_TRUE(flusher.WaitForArrival(kArrivalTimeout));

  auto first = std::async(std::launch::async, [&db] {
    return db->SetCommitDurability(LineairDB::Config::CommitDurability::Sync,
                                   kBarrierTimeout);
  });
  const auto published_by = std::chrono::steady_clock::now() + kArrivalTimeout;
  while (db->GetCommitDurability() !=
             LineairDB::Config::CommitDurability::Sync &&
         std::chrono::steady_clock::now() < published_by) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  // Long enough for the first switch to be past its epoch wait and waiting on
  // the held fdatasync; the committer below must not block that half.
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Now stop a commit inside its epoch. From here the epoch clock is frozen.
  PointHold committer_hold(kCommitPoint);
  struct DisarmCommitter {
    PointHold& hold;
    ~DisarmCommitter() { hold.Disarm(); }
  } disarm_committer{committer_hold};
  auto committer = std::async(std::launch::async, [&db] {
    auto& tx = db->BeginTransaction();
    tx.Write<int>("bob", 2);
    db->EndTransaction(tx, [](LineairDB::TxStatus) {});
  });
  ASSERT_TRUE(committer_hold.WaitForArrival(kArrivalTimeout));

  const auto started = std::chrono::steady_clock::now();
  auto queued = std::async(std::launch::async, [&db, kQueuedTimeout] {
    return db->SetCommitDurability(LineairDB::Config::CommitDurability::Sync,
                                   kQueuedTimeout);
  });

  // Hand the lock over halfway through the queued call's budget: it acquires
  // late, and the epoch it then waits for cannot arrive while the committer
  // is parked, so it must end on the rest of its own budget.
  std::this_thread::sleep_for(kQueuedTimeout / 2);
  flusher.Disarm();

  ASSERT_EQ(queued.wait_for(kArrivalTimeout), std::future_status::ready);
  const auto waited = std::chrono::steady_clock::now() - started;
  EXPECT_FALSE(queued.get());
  EXPECT_LT(waited, kQueuedTimeout + kSchedulingSlack);
  ASSERT_EQ(first.wait_for(kArrivalTimeout), std::future_status::ready);
  EXPECT_TRUE(first.get());

  // With the epoch clock running again, the same call succeeds.
  committer_hold.Disarm();
  committer.wait();
  EXPECT_TRUE(db->SetCommitDurability(
      LineairDB::Config::CommitDurability::Sync, kBarrierTimeout));

  db.reset(nullptr);
  std::filesystem::remove_all(kWorkDir);
}

// A switch to Async is not exempt from the bound: one that expires while
// queued must report failure and leave the stricter policy alone.
TEST(DurabilityBarrierTest, AnExpiredAsyncSwitchLeavesSyncInPlace) {
  std::filesystem::remove_all(kWorkDir);
  PointHold flusher(kWalPoint);

  auto db = std::make_unique<LineairDB::Database>(BarrierConfig());
  struct DisarmOnExit {
    PointHold& hold;
    ~DisarmOnExit() { hold.Disarm(); }
  } disarm_on_exit{flusher};

  ASSERT_TRUE(TestHelper::DoTransactions(
      db.get(), {[](LineairDB::Transaction& tx) {
        tx.Write<int>("alice", 1);
      }}));
  db->Fence();
  ASSERT_TRUE(flusher.WaitForArrival(kArrivalTimeout));

  auto first = std::async(std::launch::async, [&db] {
    return db->SetCommitDurability(LineairDB::Config::CommitDurability::Sync,
                                   kBarrierTimeout);
  });
  const auto published_by = std::chrono::steady_clock::now() + kArrivalTimeout;
  while (db->GetCommitDurability() !=
             LineairDB::Config::CommitDurability::Sync &&
         std::chrono::steady_clock::now() < published_by) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_EQ(first.wait_for(std::chrono::seconds(0)),
            std::future_status::timeout);

  // A budget too small to outlast the barrier ahead of it.
  EXPECT_FALSE(db->SetCommitDurability(
      LineairDB::Config::CommitDurability::Async,
      std::chrono::milliseconds(1)));
  EXPECT_EQ(db->GetCommitDurability(),
            LineairDB::Config::CommitDurability::Sync);

  flusher.Disarm();
  ASSERT_EQ(first.wait_for(kArrivalTimeout), std::future_status::ready);
  EXPECT_TRUE(first.get());
  EXPECT_EQ(db->GetCommitDurability(),
            LineairDB::Config::CommitDurability::Sync);

  db.reset(nullptr);
  std::filesystem::remove_all(kWorkDir);
}

// Everything the barrier waits for can already be true when it looks, so
// success is not enough on its own: a call held past its deadline reports
// failure rather than an acknowledgement its caller can no longer use.
TEST(DurabilityBarrierTest, ASwitchHeldPastItsDeadlineReportsFailure) {
  constexpr auto kShortTimeout = std::chrono::milliseconds(300);
  std::filesystem::remove_all(kWorkDir);
  // Only the barrier point is armed: flushing runs free, so both waits inside
  // the barrier find their targets already reached.
  PointHold hold(kBarrierPoint);

  auto db = std::make_unique<LineairDB::Database>(BarrierConfig());
  struct DisarmOnExit {
    PointHold& hold;
    ~DisarmOnExit() { hold.Disarm(); }
  } disarm_on_exit{hold};

  ASSERT_TRUE(TestHelper::DoTransactions(
      db.get(), {[](LineairDB::Transaction& tx) {
        tx.Write<int>("alice", 1);
      }}));
  db->Fence();

  auto switched = std::async(std::launch::async, [&db, kShortTimeout] {
    return db->SetCommitDurability(LineairDB::Config::CommitDurability::Sync,
                                   kShortTimeout);
  });
  ASSERT_TRUE(hold.WaitForArrival(kArrivalTimeout));

  // Held well past the deadline while the epochs turn and the log is written.
  std::this_thread::sleep_for(kShortTimeout * 3);
  hold.Release();

  ASSERT_EQ(switched.wait_for(kArrivalTimeout), std::future_status::ready);
  EXPECT_FALSE(switched.get());
  // The stricter policy stands even though the call reports failure.
  EXPECT_EQ(db->GetCommitDurability(),
            LineairDB::Config::CommitDurability::Sync);

  hold.Disarm();
  db.reset(nullptr);
  std::filesystem::remove_all(kWorkDir);
}
