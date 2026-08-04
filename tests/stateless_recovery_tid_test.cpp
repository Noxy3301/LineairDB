#include <filesystem>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "lineairdb/config.h"
#include "lineairdb/database.h"

namespace {

constexpr const char* kTable = "__anonymous_table";

LineairDB::Config MakeConfig() {
  LineairDB::Config config;
  config.max_thread = 1;
  config.epoch_duration_ms = 10;
  config.concurrency_control_protocol =
      LineairDB::Config::ConcurrencyControl::Silo;
  config.index_structure = LineairDB::Config::IndexStructure::Masstree;
  config.enable_recovery = true;
  config.enable_logging = true;
  config.enable_checkpointing = false;
  config.work_dir = "./lineairdb_stateless_recovery_tid_test_logs";
  return config;
}

// A stateless write captures its log snapshot while the row lock is held.
// A snapshot persisted with the locked TID poisons recovery: the recovered
// row looks locked by a transaction that no longer exists, and every later
// access to the key spins or aborts forever.
TEST(StatelessRecoveryTidTest, ARecoveredKeyAcceptsTheNextWrite) {
  const auto config = MakeConfig();
  std::filesystem::remove_all(config.work_dir);

  {
    LineairDB::Database db(config);
    std::string reason;
    const bool committed = db.ValidateAndCommit(
        {}, {{kTable, "alice", "v1", false}}, {}, {}, &reason);
    db.ReleaseMasstreeThreadEpoch();
    ASSERT_TRUE(committed) << reason;
  }

  {
    LineairDB::Database db(config);

    auto read = db.StatelessRead(kTable, "alice");
    db.ReleaseMasstreeThreadEpoch();
    ASSERT_TRUE(read.found);
    EXPECT_EQ(read.tid % 2, 0u)
        << "the recovered TID still carries the lock bit";

    std::string reason;
    const bool committed = db.ValidateAndCommit(
        {}, {{kTable, "alice", "v2", false}}, {}, {}, &reason);
    db.ReleaseMasstreeThreadEpoch();
    EXPECT_TRUE(committed) << reason;
  }

  std::filesystem::remove_all(MakeConfig().work_dir);
}

}  // namespace
