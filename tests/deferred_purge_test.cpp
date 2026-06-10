#include "lineairdb/database.h"

#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "lineairdb/config.h"
#include "lineairdb/stateless.h"

namespace {

constexpr const char* kTable = "__anonymous_table";

LineairDB::Config MakeConfig(size_t epoch_duration_ms) {
  LineairDB::Config config;
  config.max_thread = 1;
  config.epoch_duration_ms = epoch_duration_ms;
  config.concurrency_control_protocol =
      LineairDB::Config::ConcurrencyControl::Silo;
  config.index_structure = LineairDB::Config::IndexStructure::Masstree;
  config.enable_recovery = false;
  config.enable_logging = false;
  config.enable_checkpointing = false;
  config.work_dir = "./lineairdb_deferred_purge_test_logs";
  std::filesystem::remove_all(config.work_dir);
  return config;
}

bool CommitWrite(LineairDB::Database& db, const std::string& key,
                 const std::string& value, std::string* reason = nullptr) {
  const bool committed = db.ValidateAndCommit(
      {}, {{kTable, key, value, false}}, {}, {}, reason);
  db.ReleaseMasstreeThreadEpoch();
  return committed;
}

bool CommitDelete(LineairDB::Database& db, const std::string& key,
                  std::string* reason = nullptr) {
  const bool committed = db.ValidateAndCommit(
      {}, {{kTable, key, "", true}}, {}, {}, reason);
  db.ReleaseMasstreeThreadEpoch();
  return committed;
}

LineairDB::StatelessReadResult Read(LineairDB::Database& db,
                                    const std::string& key) {
  auto result = db.StatelessRead(kTable, key);
  db.ReleaseMasstreeThreadEpoch();
  return result;
}

bool ValidateRead(LineairDB::Database& db,
                  const LineairDB::StatelessReadResult& read,
                  const std::string& key, std::string* reason) {
  const bool committed = db.ValidateAndCommit(
      {{kTable, key, read.tid, read.found}}, {}, {}, {}, reason);
  db.ReleaseMasstreeThreadEpoch();
  return committed;
}

bool StartsWith(const std::string& value, const std::string& prefix) {
  return value.rfind(prefix, 0) == 0;
}

void WaitForEpochReaper(LineairDB::Database& db,
                        std::chrono::milliseconds duration) {
  std::this_thread::sleep_for(duration);
  db.Fence();
  db.ReleaseMasstreeThreadEpoch();
  std::this_thread::sleep_for(duration);
}

}  // namespace

TEST(DeferredPurgeTest, SameEpochDeleteReinsertInvalidatesStaleRead) {
  auto config = MakeConfig(100);
  LineairDB::Database db(config);

  ASSERT_TRUE(CommitWrite(db, "k", "v1"));
  const auto stale = Read(db, "k");
  ASSERT_TRUE(stale.found);
  ASSERT_EQ(stale.value, "v1");

  ASSERT_TRUE(CommitDelete(db, "k"));
  ASSERT_TRUE(CommitWrite(db, "k", "v2"));

  std::string reason;
  EXPECT_FALSE(ValidateRead(db, stale, "k", &reason));
}

TEST(DeferredPurgeTest, FoundReadAbortsAfterDeferredPurgeRemovesSlot) {
  auto config = MakeConfig(5);
  LineairDB::Database db(config);

  ASSERT_TRUE(CommitWrite(db, "k", "v1"));
  const auto stale = Read(db, "k");
  ASSERT_TRUE(stale.found);

  ASSERT_TRUE(CommitDelete(db, "k"));

  std::string reason;
  for (int i = 0; i < 200; ++i) {
    ASSERT_FALSE(ValidateRead(db, stale, "k", &reason));
    if (StartsWith(reason, "exact_read_disappeared")) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_TRUE(StartsWith(reason, "exact_read_disappeared")) << reason;
}

TEST(DeferredPurgeTest, ReinsertBeforeReaperKeepsLiveRow) {
  auto config = MakeConfig(200);
  LineairDB::Database db(config);

  ASSERT_TRUE(CommitWrite(db, "k", "v1"));
  ASSERT_TRUE(CommitDelete(db, "k"));
  ASSERT_TRUE(CommitWrite(db, "k", "v2"));

  WaitForEpochReaper(db, std::chrono::milliseconds(300));

  const auto live = Read(db, "k");
  EXPECT_TRUE(live.found);
  EXPECT_EQ(live.value, "v2");
}

TEST(DeferredPurgeTest, AbsentReadStillAbortsWhenRowAppears) {
  auto config = MakeConfig(100);
  LineairDB::Database db(config);

  const auto absent = Read(db, "k");
  ASSERT_FALSE(absent.found);

  ASSERT_TRUE(CommitWrite(db, "k", "v1"));

  std::string reason;
  EXPECT_FALSE(ValidateRead(db, absent, "k", &reason));
  EXPECT_TRUE(StartsWith(reason, "exact_read_appeared")) << reason;
}
