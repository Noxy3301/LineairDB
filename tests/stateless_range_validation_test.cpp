#include "lineairdb/database.h"

#include <filesystem>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "lineairdb/config.h"
#include "lineairdb/stateless.h"

namespace {

constexpr const char* kTable = "__anonymous_table";

LineairDB::Config MakeConfig() {
  LineairDB::Config config;
  config.max_thread = 1;
  config.concurrency_control_protocol =
      LineairDB::Config::ConcurrencyControl::Silo;
  config.index_structure = LineairDB::Config::IndexStructure::Masstree;
  config.enable_recovery = false;
  config.enable_logging = false;
  config.enable_checkpointing = false;
  config.work_dir = "./lineairdb_stateless_range_validation_test_logs";
  std::filesystem::remove_all(config.work_dir);
  return config;
}

bool CommitWrite(LineairDB::Database& db, const std::string& key,
                 const std::string& value) {
  const bool committed =
      db.ValidateAndCommit({}, {{kTable, key, value, false}}, {});
  db.ReleaseMasstreeThreadEpoch();
  return committed;
}

bool CommitDelete(LineairDB::Database& db, const std::string& key) {
  const bool committed = db.ValidateAndCommit({}, {{kTable, key, "", true}}, {});
  db.ReleaseMasstreeThreadEpoch();
  return committed;
}

/// Scan the range and assemble the evidence a caller submits at commit.
LineairDB::ExternalRangeReadEntry ScanRange(LineairDB::Database& db,
                                           const std::string& start_key,
                                           const std::string& end_key,
                                           uint64_t row_limit = 0,
                                           bool reverse_scan = false) {
  auto scan =
      db.StatelessRangeScan(kTable, start_key, end_key, row_limit, reverse_scan);
  db.ReleaseMasstreeThreadEpoch();
  EXPECT_TRUE(scan.ok);

  LineairDB::ExternalRangeReadEntry range;
  range.table_name = kTable;
  range.start_key = start_key;
  range.end_key = end_key;
  range.row_limit = row_limit;
  range.reverse_scan = reverse_scan;
  for (const auto& row : scan.rows) {
    range.result_keys.emplace_back(row.key);
  }
  return range;
}

bool Revalidate(LineairDB::Database& db,
                const LineairDB::ExternalRangeReadEntry& range,
                std::string* reason) {
  const bool committed = db.ValidateAndCommit({}, {}, {}, {range}, reason);
  db.ReleaseMasstreeThreadEpoch();
  return committed;
}

void SeedRows(LineairDB::Database& db) {
  for (const char* key : {"k1", "k2", "k3", "k4"}) {
    ASSERT_TRUE(CommitWrite(db, key, "v"));
  }
}

/// Materialize a key without ever initializing it. Resolving a write inserts
/// the slot before validation runs, and an aborted commit leaves it behind.
void LeaveBlankSlot(LineairDB::Database& db, const std::string& key) {
  std::string reason;
  const bool committed =
      db.ValidateAndCommit({{kTable, "k1", 0, true}},
                           {{kTable, key, "v", false}}, {}, {}, &reason);
  db.ReleaseMasstreeThreadEpoch();
  ASSERT_FALSE(committed) << "the write was supposed to abort";
}

}  // namespace

TEST(StatelessRangeValidationTest, AnUnchangedRangeCommits) {
  auto config = MakeConfig();
  LineairDB::Database db(config);
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5");
  ASSERT_EQ(range.result_keys,
            (std::vector<std::string>{"k1", "k2", "k3", "k4"}));

  std::string reason;
  EXPECT_TRUE(Revalidate(db, range, &reason)) << reason;
}

TEST(StatelessRangeValidationTest, ARowDeletedInsideTheRangeAborts) {
  auto config = MakeConfig();
  LineairDB::Database db(config);
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5");
  ASSERT_TRUE(CommitDelete(db, "k2"));

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, &reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}

TEST(StatelessRangeValidationTest, ARowDeletedAtTheEndOfTheRangeAborts) {
  // The replay is a strict prefix of the evidence, so nothing diverges
  // positionally and only the length check rejects it.
  auto config = MakeConfig();
  LineairDB::Database db(config);
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5");
  ASSERT_TRUE(CommitDelete(db, "k4"));

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, &reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}

TEST(StatelessRangeValidationTest, ARowInsertedInsideTheRangeAborts) {
  auto config = MakeConfig();
  LineairDB::Database db(config);
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5");
  ASSERT_TRUE(CommitWrite(db, "k25", "v"));

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, &reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}

TEST(StatelessRangeValidationTest, ARowInsertedAtTheEndOfTheRangeAborts) {
  // The evidence is a strict prefix of the replay, so the divergence is the
  // first live row past the evidence.
  auto config = MakeConfig();
  LineairDB::Database db(config);
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5");
  ASSERT_TRUE(CommitWrite(db, "k45", "v"));

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, &reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}

TEST(StatelessRangeValidationTest, ALimitedRangeIgnoresChangesPastItsCap) {
  auto config = MakeConfig();
  LineairDB::Database db(config);
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5", 2);
  ASSERT_EQ(range.result_keys, (std::vector<std::string>{"k1", "k2"}));
  ASSERT_TRUE(CommitWrite(db, "k45", "v"));

  std::string reason;
  EXPECT_TRUE(Revalidate(db, range, &reason)) << reason;
}

TEST(StatelessRangeValidationTest, ANonLiveSlotDoesNotConsumeTheCap) {
  // The cap counts live rows. A blank slot between the first two of them must
  // leave the replay room to reach the second.
  auto config = MakeConfig();
  LineairDB::Database db(config);
  SeedRows(db);
  LeaveBlankSlot(db, "k15");

  const auto range = ScanRange(db, "k1", "k5", 2);
  ASSERT_EQ(range.result_keys, (std::vector<std::string>{"k1", "k2"}));

  std::string reason;
  EXPECT_TRUE(Revalidate(db, range, &reason)) << reason;
}

TEST(StatelessRangeValidationTest, AnEmptyRangeCommits) {
  auto config = MakeConfig();
  LineairDB::Database db(config);
  SeedRows(db);

  const auto range = ScanRange(db, "m1", "m9");
  ASSERT_TRUE(range.result_keys.empty());

  std::string reason;
  EXPECT_TRUE(Revalidate(db, range, &reason)) << reason;
}

TEST(StatelessRangeValidationTest, ARowAppearingInAnEmptyRangeAborts) {
  auto config = MakeConfig();
  LineairDB::Database db(config);
  SeedRows(db);

  const auto range = ScanRange(db, "m1", "m9");
  ASSERT_TRUE(range.result_keys.empty());
  ASSERT_TRUE(CommitWrite(db, "m5", "v"));

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, &reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}

TEST(StatelessRangeValidationTest, EvidenceRepeatingAKeyAborts) {
  // A primary index cannot return the same key twice, so evidence that does
  // is rejected rather than matched by the positional walk.
  auto config = MakeConfig();
  LineairDB::Database db(config);
  SeedRows(db);

  auto range = ScanRange(db, "k1", "k5");
  range.result_keys.insert(range.result_keys.begin(), "k1");

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, &reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}

TEST(StatelessRangeValidationTest, AReverseRangeAbortsOnTheSameChange) {
  auto config = MakeConfig();
  LineairDB::Database db(config);
  SeedRows(db);

  const auto range = ScanRange(db, "k1", "k5", 0, true);
  ASSERT_EQ(range.result_keys,
            (std::vector<std::string>{"k4", "k3", "k2", "k1"}));
  ASSERT_TRUE(CommitDelete(db, "k2"));

  std::string reason;
  EXPECT_FALSE(Revalidate(db, range, &reason));
  EXPECT_EQ(reason, "primary_range_result_changed");
}
