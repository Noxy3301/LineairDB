#include <errno.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "recovery/logger.h"
#include "recovery/wal.h"
#include "types/snapshot.hpp"

namespace {

using LineairDB::EpochNumber;
using LineairDB::Snapshot;
using LineairDB::WriteSetType;
using LineairDB::Recovery::Logger;
using LineairDB::Recovery::WalIo;

constexpr auto kTestTimeout = std::chrono::seconds(5);

// Exercises the logger without constructing a Database: the WAL and the
// durability frontier are the units under test here, and a Database would drag
// in the epoch framework and the thread pool.
class LoggerDurabilityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "lineairdb_logger_XXXXXX")
            .string();
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    ASSERT_NE(::mkdtemp(buffer.data()), nullptr);
    root_ = buffer.data();
    config_.work_dir = root_ + "/logs";
    config_.commit_durability = LineairDB::Config::CommitDurability::Async;
    config_.enable_checkpointing = false;
    // Every fixture writes out its capacity before its first group; these
    // logs hold a handful of frames.
    config_.wal_initial_capacity_bytes = 1ull << 20;
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(root_, ec);
  }

  static WriteSetType MakeWriteSet(const std::string& key) {
    Snapshot snapshot(key, nullptr, 0, nullptr, "t", "");
    WriteSetType write_set;
    write_set.emplace_back(std::move(snapshot));
    return write_set;
  }

  /** A write set of secondary snapshots with no delta persists nothing. */
  static WriteSetType MakeEmptySecondaryWriteSet(const std::string& key) {
    Snapshot snapshot(key, nullptr, 0, nullptr, "t", "idx");
    WriteSetType write_set;
    write_set.emplace_back(std::move(snapshot));
    return write_set;
  }

  std::string root_;
  LineairDB::Config config_;
};

TEST_F(LoggerDurabilityTest, EnqueueReportsOnlyWhatItPersists) {
  Logger logger(config_);
  EXPECT_TRUE(logger.Enqueue(MakeWriteSet("alice"), 5));
  EXPECT_FALSE(logger.Enqueue(WriteSetType{}, 5));
  EXPECT_FALSE(logger.Enqueue(MakeEmptySecondaryWriteSet("bob"), 5));
}

TEST_F(LoggerDurabilityTest, AlreadyDurableReturnsImmediately) {
  Logger logger(config_);
  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::Ok);
  logger.StartFlusher();

  ASSERT_TRUE(logger.Enqueue(MakeWriteSet("alice"), 5));
  logger.ScheduleFlush(5);

  EXPECT_EQ(logger.WaitUntilDurable(5, Logger::Deadline::max()),
            Logger::WaitResult::Durable);
  EXPECT_EQ(logger.GetDurableEpoch(), 5u);
  // A second wait on a frontier already reached must not block at all.
  EXPECT_EQ(logger.WaitUntilDurable(5, std::chrono::steady_clock::now()),
            Logger::WaitResult::Durable);
  logger.StopAndDrainFlusher();
}

TEST_F(LoggerDurabilityTest, WaitersWakeAtEpochGranularity) {
  Logger logger(config_);
  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::Ok);
  logger.StartFlusher();

  ASSERT_TRUE(logger.Enqueue(MakeWriteSet("alice"), 5));
  ASSERT_TRUE(logger.Enqueue(MakeWriteSet("bob"), 7));

  auto wait_for = [&logger](EpochNumber epoch) {
    return std::async(std::launch::async, [&logger, epoch] {
      return logger.WaitUntilDurable(epoch, Logger::Deadline::max());
    });
  };
  auto first = wait_for(5);
  auto second = wait_for(5);
  auto later = wait_for(7);

  logger.ScheduleFlush(5);
  ASSERT_EQ(first.wait_for(kTestTimeout), std::future_status::ready);
  ASSERT_EQ(second.wait_for(kTestTimeout), std::future_status::ready);
  EXPECT_EQ(first.get(), Logger::WaitResult::Durable);
  EXPECT_EQ(second.get(), Logger::WaitResult::Durable);
  // Epoch 7 is not covered by a flush through 5.
  EXPECT_EQ(later.wait_for(std::chrono::milliseconds(200)),
            std::future_status::timeout);

  logger.ScheduleFlush(7);
  ASSERT_EQ(later.wait_for(kTestTimeout), std::future_status::ready);
  EXPECT_EQ(later.get(), Logger::WaitResult::Durable);
  logger.StopAndDrainFlusher();
}

// The acknowledgement a Sync commit waits for cannot be given while the
// fdatasync that would earn it is still running. Holding the syscall makes the
// order observable rather than merely likely.
TEST_F(LoggerDurabilityTest, SyncAcknowledgementFollowsTheFdatasync) {
  config_.commit_durability = LineairDB::Config::CommitDurability::Sync;

  std::mutex mutex;
  std::condition_variable held;
  bool inside_fdatasync = false;
  bool released = false;

  WalIo io = WalIo::Posix();
  auto posix_fdatasync = io.fdatasync;
  io.fdatasync = [&](int fd) {
    std::unique_lock<std::mutex> lock(mutex);
    inside_fdatasync = true;
    held.notify_all();
    held.wait(lock, [&] { return released; });
    lock.unlock();
    return posix_fdatasync(fd);
  };

  Logger logger(config_, io);
  std::atomic<bool> committer_started{false};
  std::future<void> committer;

  // Releases a held fdatasync and drains the flusher on every exit path: an
  // assertion failure would otherwise leave the committer waiting forever,
  // and the future's destructor would block before ~Logger could wake it.
  // Declared after the future so unwinding runs the guard first; the drain
  // wakes the committer with either the durable epoch or the stopped state.
  struct ReleaseOnExit {
    Logger& logger;
    std::mutex& mutex;
    std::condition_variable& held;
    bool& released;
    ~ReleaseOnExit() {
      {
        std::lock_guard<std::mutex> lock(mutex);
        released = true;
      }
      held.notify_all();
      logger.StopAndDrainFlusher();
    }
  } release_on_exit{logger, mutex, held, released};

  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::Ok);
  logger.StartFlusher();

  ASSERT_TRUE(logger.Enqueue(MakeWriteSet("alice"), 3));
  committer = std::async(std::launch::async, [&logger, &committer_started] {
    committer_started.store(true);
    logger.AwaitCommitDurability(3, true);
  });
  // The timeout probe below measures the wait, not thread startup: flush only
  // once the committer thread is provably running.
  while (!committer_started.load()) std::this_thread::yield();
  logger.ScheduleFlush(3);

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(held.wait_for(lock, kTestTimeout,
                              [&] { return inside_fdatasync; }));
  }
  EXPECT_EQ(committer.wait_for(std::chrono::milliseconds(200)),
            std::future_status::timeout);
  EXPECT_EQ(logger.GetDurableEpoch(), 0u);

  {
    std::lock_guard<std::mutex> lock(mutex);
    released = true;
  }
  held.notify_all();

  ASSERT_EQ(committer.wait_for(kTestTimeout), std::future_status::ready);
  committer.get();
  EXPECT_EQ(logger.GetDurableEpoch(), 3u);
  logger.StopAndDrainFlusher();
}

// A caller that decided not to wait returns at once, whether because the
// contract is Async or because the transaction left no record.
TEST_F(LoggerDurabilityTest, AsyncAndUnloggedCommitsDoNotWait) {
  {
    Logger logger(config_);
    ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::Ok);
    logger.StartFlusher();

    // Async: the commit path decided not to wait, and an epoch that will
    // never be flushed still returns at once.
    logger.AwaitCommitDurability(99, false);
    EXPECT_EQ(logger.GetDurableEpoch(), 0u);
    logger.StopAndDrainFlusher();
  }

  // The log is held exclusively for as long as a logger owns it, so the
  // second contract gets its own scope rather than overlapping with the
  // first.
  config_.commit_durability = LineairDB::Config::CommitDurability::Sync;
  Logger sync_logger(config_);
  ASSERT_EQ(sync_logger.Recover().status, Logger::RecoveryStatus::Ok);
  sync_logger.StartFlusher();

  // Sync, but nothing was enqueued: there is no record to wait for.
  sync_logger.AwaitCommitDurability(99, false);
  EXPECT_EQ(sync_logger.GetDurableEpoch(), 0u);
  sync_logger.StopAndDrainFlusher();
}

// The policy is switchable while the logger runs: a commit that captured
// Async does not wait, and one that captured Sync afterwards waits for the
// fdatasync that makes its epoch durable.
TEST_F(LoggerDurabilityTest, CommitDurabilityIsSwitchableAtRuntime) {
  std::mutex mutex;
  std::condition_variable held;
  bool inside_fdatasync = false;
  bool released = false;

  WalIo io = WalIo::Posix();
  auto posix_fdatasync = io.fdatasync;
  io.fdatasync = [&](int fd) {
    std::unique_lock<std::mutex> lock(mutex);
    inside_fdatasync = true;
    held.notify_all();
    held.wait(lock, [&] { return released; });
    lock.unlock();
    return posix_fdatasync(fd);
  };

  Logger logger(config_, io);
  EXPECT_EQ(logger.GetCommitDurability(),
            LineairDB::Config::CommitDurability::Async);

  std::atomic<bool> committer_started{false};
  std::future<void> committer;

  // See SyncAcknowledgementFollowsTheFdatasync: releases the held fdatasync
  // and drains the flusher on every exit path, so a failed assertion cannot
  // leave the committer waiting forever.
  struct ReleaseOnExit {
    Logger& logger;
    std::mutex& mutex;
    std::condition_variable& held;
    bool& released;
    ~ReleaseOnExit() {
      {
        std::lock_guard<std::mutex> lock(mutex);
        released = true;
      }
      held.notify_all();
      logger.StopAndDrainFlusher();
    }
  } release_on_exit{logger, mutex, held, released};

  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::Ok);
  logger.StartFlusher();

  // Async: the decision the commit captured is "do not wait", and an epoch
  // that will never be flushed still returns at once.
  ASSERT_TRUE(logger.Enqueue(MakeWriteSet("alice"), 3));
  logger.AwaitCommitDurability(3, false);
  EXPECT_EQ(logger.GetDurableEpoch(), 0u);

  logger.SetCommitDurability(LineairDB::Config::CommitDurability::Sync);
  EXPECT_EQ(logger.GetCommitDurability(),
            LineairDB::Config::CommitDurability::Sync);

  ASSERT_TRUE(logger.Enqueue(MakeWriteSet("bob"), 3));
  committer = std::async(std::launch::async, [&logger, &committer_started] {
    committer_started.store(true);
    logger.AwaitCommitDurability(3, true);
  });
  while (!committer_started.load()) std::this_thread::yield();
  logger.ScheduleFlush(3);

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(held.wait_for(lock, kTestTimeout,
                              [&] { return inside_fdatasync; }));
  }
  EXPECT_EQ(committer.wait_for(std::chrono::milliseconds(200)),
            std::future_status::timeout);
  EXPECT_EQ(logger.GetDurableEpoch(), 0u);

  {
    std::lock_guard<std::mutex> lock(mutex);
    released = true;
  }
  held.notify_all();

  ASSERT_EQ(committer.wait_for(kTestTimeout), std::future_status::ready);
  committer.get();
  EXPECT_EQ(logger.GetDurableEpoch(), 3u);

  // And back: a commit that captures Async after the switch does not wait.
  logger.SetCommitDurability(LineairDB::Config::CommitDurability::Async);
  ASSERT_TRUE(logger.Enqueue(MakeWriteSet("carol"), 9));
  logger.AwaitCommitDurability(9, false);
  EXPECT_EQ(logger.GetDurableEpoch(), 3u);

  logger.StopAndDrainFlusher();
}

// With the fail-stop armed, a write failure ends the process by abort: under
// Async nobody waits on the frontier, and a process that carried on would keep
// acknowledging commits that exist only in memory. The rest of this file
// constructs loggers without arming: there an I/O failure surfaces as
// WaitResult::Failed instead of ending the process.
TEST_F(LoggerDurabilityTest, ArmedFailStopEndsTheProcessOnASyncFailure) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";

  WalIo io = WalIo::Posix();
  io.fdatasync = [](int) {
    errno = EIO;
    return -1;
  };

  // Everything lives inside the child: a flusher started in the parent would
  // not survive the fork the death test performs. Armed before the flusher
  // starts, in the database's order.
  EXPECT_EXIT(
      {
        Logger logger(config_, io);
        logger.Recover();
        logger.EnableProcessFailStop();
        logger.StartFlusher();
        logger.Enqueue(MakeWriteSet("alice"), 3);
        logger.ScheduleFlush(3);
        std::this_thread::sleep_for(kTestTimeout);
      },
      ::testing::KilledBySignal(SIGABRT), "");
}

TEST_F(LoggerDurabilityTest, RecordsAboveTheTargetAreCarriedForward) {
  {
    Logger logger(config_);
    ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::Ok);
    logger.StartFlusher();
    ASSERT_TRUE(logger.Enqueue(MakeWriteSet("alice"), 4));
    ASSERT_TRUE(logger.Enqueue(MakeWriteSet("bob"), 9));
    logger.ScheduleFlush(4);
    ASSERT_EQ(logger.WaitUntilDurable(4, Logger::Deadline::max()),
              Logger::WaitResult::Durable);
    logger.ScheduleFlush(9);
    ASSERT_EQ(logger.WaitUntilDurable(9, Logger::Deadline::max()),
              Logger::WaitResult::Durable);
    logger.StopAndDrainFlusher();
  }

  // Both epochs must be present, in order, after reopening.
  LineairDB::Recovery::Wal wal(config_.work_dir,
                              LineairDB::Recovery::WalIo::Posix(),
                              config_.wal_initial_capacity_bytes);
  const auto scan = wal.ScanAndRepair();
  ASSERT_EQ(scan.status, LineairDB::Recovery::WalScanResult::Status::Ok);
  EXPECT_EQ(scan.frontier, 9u);
  ASSERT_EQ(scan.records.size(), 2u);
  EXPECT_EQ(scan.records[0].epoch, 4u);
  EXPECT_EQ(scan.records[1].epoch, 9u);
}

TEST_F(LoggerDurabilityTest, StopWakesEveryWaiter) {
  Logger logger(config_);
  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::Ok);
  logger.StartFlusher();

  auto first = std::async(std::launch::async, [&logger] {
    return logger.WaitUntilDurable(11, Logger::Deadline::max());
  });
  auto second = std::async(std::launch::async, [&logger] {
    return logger.WaitUntilDurable(12, Logger::Deadline::max());
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  logger.StopAndDrainFlusher();
  ASSERT_EQ(first.wait_for(kTestTimeout), std::future_status::ready);
  ASSERT_EQ(second.wait_for(kTestTimeout), std::future_status::ready);
  EXPECT_EQ(first.get(), Logger::WaitResult::Stopped);
  EXPECT_EQ(second.get(), Logger::WaitResult::Stopped);
}

TEST_F(LoggerDurabilityTest, FdatasyncFailureHoldsTheFrontierAndFailsWaiters) {
  WalIo io = WalIo::Posix();
  io.fdatasync = [](int) {
    errno = EIO;
    return -1;
  };

  Logger logger(config_, io);
  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::Ok);
  logger.StartFlusher();

  ASSERT_TRUE(logger.Enqueue(MakeWriteSet("alice"), 3));
  auto waiting = std::async(std::launch::async, [&logger] {
    return logger.WaitUntilDurable(3, Logger::Deadline::max());
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  logger.ScheduleFlush(3);
  ASSERT_EQ(waiting.wait_for(kTestTimeout), std::future_status::ready);
  EXPECT_EQ(waiting.get(), Logger::WaitResult::Failed);
  // The frontier must not move: durability was never confirmed, whatever
  // bytes may have landed.
  EXPECT_EQ(logger.GetDurableEpoch(), 0u);

  // A waiter arriving after the failure learns of it rather than blocking.
  EXPECT_EQ(logger.WaitUntilDurable(3, Logger::Deadline::max()),
            Logger::WaitResult::Failed);
  logger.StopAndDrainFlusher();
}

TEST_F(LoggerDurabilityTest, WriteFailureFailsWaiters) {
  WalIo io = WalIo::Posix();
  io.pwrite = [](int, const void*, size_t, off_t) -> ssize_t {
    errno = EIO;
    return -1;
  };

  Logger logger(config_, io);
  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::Ok);
  logger.StartFlusher();

  ASSERT_TRUE(logger.Enqueue(MakeWriteSet("alice"), 3));
  logger.ScheduleFlush(3);
  EXPECT_EQ(logger.WaitUntilDurable(3, Logger::Deadline::max()),
            Logger::WaitResult::Failed);
  EXPECT_EQ(logger.GetDurableEpoch(), 0u);
  logger.StopAndDrainFlusher();
}

TEST_F(LoggerDurabilityTest, TimeoutIsReportedWhenNothingIsScheduled) {
  Logger logger(config_);
  ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::Ok);
  logger.StartFlusher();

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
  EXPECT_EQ(logger.WaitUntilDurable(42, deadline),
            Logger::WaitResult::TimedOut);
  logger.StopAndDrainFlusher();
}

TEST_F(LoggerDurabilityTest, StopDrainsWhatWasAlreadyClosed) {
  {
    Logger logger(config_);
    ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::Ok);
    logger.StartFlusher();
    ASSERT_TRUE(logger.Enqueue(MakeWriteSet("alice"), 6));
    logger.ScheduleFlush(6);
    // Stop without waiting: the drain must still write epoch 6.
    logger.StopAndDrainFlusher();
    EXPECT_EQ(logger.GetDurableEpoch(), 6u);
  }

  LineairDB::Recovery::Wal wal(config_.work_dir,
                              LineairDB::Recovery::WalIo::Posix(),
                              config_.wal_initial_capacity_bytes);
  const auto scan = wal.ScanAndRepair();
  ASSERT_EQ(scan.status, LineairDB::Recovery::WalScanResult::Status::Ok);
  EXPECT_EQ(scan.frontier, 6u);
}

TEST_F(LoggerDurabilityTest, RecoverReportsTheFrontierOfAnExistingLog) {
  {
    Logger logger(config_);
    ASSERT_EQ(logger.Recover().status, Logger::RecoveryStatus::Ok);
    logger.StartFlusher();
    ASSERT_TRUE(logger.Enqueue(MakeWriteSet("alice"), 8));
    logger.ScheduleFlush(8);
    ASSERT_EQ(logger.WaitUntilDurable(8, Logger::Deadline::max()),
              Logger::WaitResult::Durable);
    logger.StopAndDrainFlusher();
  }

  Logger reopened(config_);
  const auto recovered = reopened.Recover();
  ASSERT_EQ(recovered.status, Logger::RecoveryStatus::Ok);
  EXPECT_EQ(recovered.frontier, 8u);
  EXPECT_EQ(reopened.GetDurableEpoch(), 8u);
  ASSERT_EQ(recovered.recovery_set.size(), 1u);
  EXPECT_EQ(recovered.recovery_set[0].key, "alice");
}

}  // namespace
