#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <string>
#include <vector>

#include "lineairdb/config.h"
#include "lineairdb/database.h"
#include "lineairdb/stateless.h"
#include "recovery/wal.h"

namespace {

constexpr const char* kTable = "__anonymous_table";
constexpr const char* kIndex = "idx";

using LineairDB::Recovery::Wal;
using LineairDB::Recovery::WalScanResult;

// The stateless commit path is what proxy traffic takes, and what it writes to
// the log is only observable after the instance that wrote it is gone: the log
// is held under an exclusive lock while a Database is open.
class StatelessRecoveryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "lineairdb_stateless_XXXXXX")
            .string();
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    ASSERT_NE(::mkdtemp(buffer.data()), nullptr);
    root_ = buffer.data();
    work_dir_ = root_ + "/logs";
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(root_, ec);
  }

  LineairDB::Config MakeConfig(bool enable_recovery) const {
    LineairDB::Config config;
    config.max_thread = 1;
    config.epoch_duration_ms = 10;
    config.concurrency_control_protocol =
        LineairDB::Config::ConcurrencyControl::Silo;
    config.index_structure = LineairDB::Config::IndexStructure::Masstree;
    config.commit_durability = LineairDB::Config::CommitDurability::Sync;
    config.enable_checkpointing = false;
    config.enable_recovery = enable_recovery;
    config.work_dir = work_dir_;
    config.wal_initial_capacity_bytes = 1ull << 20;
    return config;
  }

  static bool CommitWrite(LineairDB::Database& db, const std::string& key,
                          const std::string& value) {
    const bool committed =
        db.ValidateAndCommit({}, {{kTable, key, value, false}}, {}, {});
    db.ReleaseMasstreeThreadEpoch();
    return committed;
  }

  static bool CommitWriteWithIndexEntry(LineairDB::Database& db,
                                        const std::string& key,
                                        const std::string& value,
                                        const std::string& secondary_key) {
    const bool committed = db.ValidateAndCommit(
        {}, {{kTable, key, value, false}},
        {{kTable, kIndex, secondary_key, key, false}}, {});
    db.ReleaseMasstreeThreadEpoch();
    return committed;
  }

  static LineairDB::StatelessReadResult Read(LineairDB::Database& db,
                                             const std::string& key) {
    auto result = db.StatelessRead(kTable, key);
    db.ReleaseMasstreeThreadEpoch();
    return result;
  }

  std::string root_;
  std::string work_dir_;
};

TEST_F(StatelessRecoveryTest, ALoggedWriteCarriesTheUnlockedTid) {
  {
    auto config = MakeConfig(false);
    LineairDB::Database db(config);
    ASSERT_TRUE(db.CreateSecondaryIndex(kTable, kIndex, 0));
    ASSERT_TRUE(CommitWriteWithIndexEntry(db, "k", "v1", "s"));
  }

  Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), 1ull << 20);
  auto scan = wal.ScanAndRepair();
  ASSERT_EQ(scan.status, WalScanResult::Status::Ok);

  bool seen_row = false;
  bool seen_index_entry = false;
  for (const auto& record : scan.records) {
    for (const auto& kvp : record.key_value_pairs) {
      if (kvp.index_name.empty()) {
        if (kvp.key != "k") continue;
        seen_row = true;
        EXPECT_EQ(kvp.buffer, "v1");
      } else {
        if (kvp.key != "s") continue;
        seen_index_entry = true;
        EXPECT_EQ(kvp.secondary_primary_key, "k");
      }
      // An odd tid is a write lock held by a transaction that no longer
      // exists; recovery installs it verbatim and every later reader and
      // writer of the key waits on it forever.
      EXPECT_EQ(kvp.tid.tid % 2, 0u);
      EXPECT_NE(kvp.tid.tid, 0u);
      // The snapshot is taken before the unlock, so an epoch that advanced
      // under the lock would be recorded one epoch behind the frame.
      EXPECT_EQ(kvp.tid.epoch, record.epoch);
    }
  }
  EXPECT_TRUE(seen_row);
  EXPECT_TRUE(seen_index_entry);
}

TEST_F(StatelessRecoveryTest, ARecoveredKeyAcceptsAFurtherWrite) {
  {
    auto config = MakeConfig(false);
    LineairDB::Database db(config);
    ASSERT_TRUE(CommitWrite(db, "k", "v1"));
  }

  // Guarded by the assertion above: a locked TID in the log makes the read
  // and the write below spin rather than fail.
  {
    Wal wal(work_dir_, LineairDB::Recovery::WalIo::Posix(), 1ull << 20);
    auto scan = wal.ScanAndRepair();
    ASSERT_EQ(scan.status, WalScanResult::Status::Ok);
    for (const auto& record : scan.records) {
      for (const auto& kvp : record.key_value_pairs) {
        if (kvp.key == "k") {
          ASSERT_EQ(kvp.tid.tid % 2, 0u);
        }
      }
    }
  }

  auto config = MakeConfig(true);
  LineairDB::Database db(config);
  const auto recovered = Read(db, "k");
  EXPECT_TRUE(recovered.found);
  EXPECT_EQ(recovered.value, "v1");

  ASSERT_TRUE(CommitWrite(db, "k", "v2"));
  const auto rewritten = Read(db, "k");
  EXPECT_TRUE(rewritten.found);
  EXPECT_EQ(rewritten.value, "v2");
}

}  // namespace
