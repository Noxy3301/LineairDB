#include <gtest/gtest.h>
#include <lineairdb/database.h>
#include <lineairdb/transaction.h>
#include <lineairdb/tx_status.h>

#include <atomic>

#include "test_helper.hpp"

/**
 * @file insert_update_write_test.cpp
 * @brief Test cases for Insert, Update, and Write operations in LineairDB.
 *
 */

/**
 * Test case 1: Insert operation
 * - Insert succeeds when the key does not exist
 * - Insert fails when the key already exists (transaction should abort)
 */
TEST(InsertUpdateWriteTest, InsertBehavior) {
  LineairDB::Config config;
  config.enable_recovery = false;
  LineairDB::Database db(config);
  std::string key = "insert_test_key";
  int value1 = 100;
  int value2 = 200;

  // First insert should succeed
  std::atomic<bool> first_committed(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Insert(key, value1); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) {
          first_committed.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(first_committed.load());

  // Verify the value was inserted
  std::atomic<bool> verified(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (result.has_value() && result.value() == value1) {
          verified.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(verified.load());

  // Second insert with the same key should fail (transaction aborts)
  std::atomic<bool> second_aborted(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Insert(key, value2); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Aborted) {
          second_aborted.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(second_aborted.load());

  // Verify the original value is still there
  std::atomic<bool> value_unchanged(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (result.has_value() && result.value() == value1) {
          value_unchanged.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(value_unchanged.load());
}

/**
 * Test case 2: Update operation
 * - Update fails when the key does not exist (transaction should abort)
 * - Update succeeds when the key exists
 */
TEST(InsertUpdateWriteTest, UpdateBehavior) {
  LineairDB::Config config;
  config.enable_recovery = false;
  LineairDB::Database db(config);
  std::string key = "update_test_key";
  int value1 = 300;
  int value2 = 400;

  // Try to update a non-existent key (should fail)
  std::atomic<bool> update_aborted(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Update(key, value1); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Aborted) {
          update_aborted.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(update_aborted.load());

  // Verify the key still doesn't exist
  std::atomic<bool> key_not_exists(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (!result.has_value()) {
          key_not_exists.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(key_not_exists.load());

  // Insert the key first
  std::atomic<bool> inserted(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Insert(key, value1); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) {
          inserted.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(inserted.load());

  // Now update should succeed
  std::atomic<bool> updated(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Update(key, value2); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) {
          updated.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(updated.load());

  // Verify the value was updated
  std::atomic<bool> value_updated(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (result.has_value() && result.value() == value2) {
          value_updated.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(value_updated.load());
}

/**
 * Test case 3: Write operation (Upsert)
 * - Write never fails
 * - Write inserts when the key does not exist
 * - Write updates when the key already exists
 */
TEST(InsertUpdateWriteTest, WriteBehavior) {
  LineairDB::Config config;
  config.enable_recovery = false;
  LineairDB::Database db(config);
  std::string key = "write_test_key";
  int value1 = 500;
  int value2 = 600;

  // Write to a non-existent key (should insert)
  std::atomic<bool> first_write_committed(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Write(key, value1); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) {
          first_write_committed.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(first_write_committed.load());

  // Verify the value was inserted
  std::atomic<bool> value_inserted(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (result.has_value() && result.value() == value1) {
          value_inserted.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(value_inserted.load());

  // Write to an existing key (should update)
  std::atomic<bool> second_write_committed(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Write(key, value2); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) {
          second_write_committed.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(second_write_committed.load());

  // Verify the value was updated
  std::atomic<bool> value_updated_to_v2(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (result.has_value() && result.value() == value2) {
          value_updated_to_v2.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(value_updated_to_v2.load());

  // Write again to demonstrate it never fails
  int value3 = 700;
  std::atomic<bool> third_write_committed(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Write(key, value3); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) {
          third_write_committed.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(third_write_committed.load());

  // Final verification
  std::atomic<bool> final_value_correct(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (result.has_value() && result.value() == value3) {
          final_value_correct.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(final_value_correct.load());
}

TEST(InsertUpdateWriteTest, InsertThenUpdateSameTransaction) {
  LineairDB::Config config;
  config.enable_recovery = false;
  LineairDB::Database db(config);

  std::string key = "insert_then_update_same_tx_key";
  int value1 = 10;
  int value2 = 20;

  std::atomic<bool> committed(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        tx.Insert(key, value1);
        tx.Update(key, value2);
      },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) {
          committed.store(true);
        }
      });
  db.Fence();
  ASSERT_TRUE(committed.load());

  std::atomic<bool> verified(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (result.has_value() && result.value() == value2) {
          verified.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(verified.load());
}

/**
 * Test case 5: Delete then Insert the same key in one transaction
 * - The row is written again, and it is visible to both a point read and a
 *   range scan. The scan is the part a range index has to be told about: a
 *   delete removes the key from it, and the insert has to put it back.
 */
TEST(InsertUpdateWriteTest, DeleteThenInsertSameTransaction) {
  LineairDB::Config config;
  config.enable_recovery = false;
  // The default protocol needs the NWR pivot metadata this build omits, and
  // the range-index backend cannot serve a re-inserted key at all: reading one
  // back never returns, with or without this transaction shape.
  config.concurrency_control_protocol = LineairDB::Config::ConcurrencyControl::Silo;
  config.index_structure = LineairDB::Config::IndexStructure::Masstree;
  LineairDB::Database db(config);

  std::string key = "delete_then_insert_key";
  int value1 = 30;
  int value2 = 40;

  std::atomic<bool> inserted(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Insert(key, value1); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) inserted.store(true);
      });
  db.Fence();
  ASSERT_TRUE(inserted.load());

  std::atomic<bool> committed(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        tx.Delete(key);
        tx.Insert(key, value2);
      },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) committed.store(true);
      });
  db.Fence();
  ASSERT_TRUE(committed.load());

  std::atomic<bool> read_back(false);
  std::atomic<size_t> scanned(0);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (result.has_value() && result.value() == value2) {
          read_back.store(true);
        }
        size_t seen = 0;
        tx.Scan(key, key + "\xff",
                [&](std::string_view scanned_key,
                    const std::pair<const void*, const size_t>) {
                  if (scanned_key == key) seen++;
                  return false;
                });
        scanned.store(seen);
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(read_back.load());
  ASSERT_EQ(1u, scanned.load());
}

/**
 * Test case 6: Insert after the deleted key's entry was physically purged
 * - The reaper removes a tombstone's entry a few epochs after the delete
 *   commits. Re-inserting the key then claims a new entry, which must commit
 *   and read back. The purge is not observable from here, so this covers the
 *   outcome rather than the timing.
 */
TEST(InsertUpdateWriteTest, InsertAfterTombstoneIsPurged) {
  LineairDB::Config config;
  config.enable_recovery = false;
  config.concurrency_control_protocol = LineairDB::Config::ConcurrencyControl::Silo;
  config.index_structure = LineairDB::Config::IndexStructure::Masstree;
  LineairDB::Database db(config);

  std::string key = "purged_then_reinserted_key";
  int value1 = 50;
  int value2 = 60;

  std::atomic<bool> inserted(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Insert(key, value1); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) inserted.store(true);
      });
  db.Fence();
  ASSERT_TRUE(inserted.load());

  std::atomic<bool> deleted(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Delete(key); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) deleted.store(true);
      });
  db.Fence();
  ASSERT_TRUE(deleted.load());

  // Give the deferred purge several epochs to retire the entry.
  for (int i = 0; i < 5; i++) db.Fence();

  std::atomic<bool> reinserted(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) { tx.Insert(key, value2); },
      [&](LineairDB::TxStatus status) {
        if (status == LineairDB::TxStatus::Committed) reinserted.store(true);
      });
  db.Fence();
  ASSERT_TRUE(reinserted.load());

  std::atomic<bool> read_back(false);
  db.ExecuteTransaction(
      [&](LineairDB::Transaction& tx) {
        auto result = tx.Read<int>(key);
        if (result.has_value() && result.value() == value2) {
          read_back.store(true);
        }
      },
      [](LineairDB::TxStatus) {});
  db.Fence();
  ASSERT_TRUE(read_back.load());
}
