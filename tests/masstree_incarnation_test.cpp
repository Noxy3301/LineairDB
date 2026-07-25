#include <lineairdb/config.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"
#include "index/impl/masstree_index.hpp"
#include "index/index_base.h"
#include "util/epoch_framework.hpp"

namespace {

using LineairDB::Index::MasstreeIndex;
using LineairDB::Index::NodeVersionEntry;

std::string OrderedKey(std::uint64_t value) {
  std::string key(sizeof(value), '\0');
  for (size_t i = 0; i < sizeof(value); ++i) {
    key[sizeof(value) - i - 1] = static_cast<char>(value & 0xff);
    value >>= 8;
  }
  return key;
}

std::unique_ptr<MasstreeIndex> MakeIndex(LineairDB::EpochFramework& epoch) {
  LineairDB::Config config;
  config.index_structure = LineairDB::Config::IndexStructure::Masstree;
  return std::make_unique<MasstreeIndex>(config, epoch);
}

void InsertKeys(MasstreeIndex& index,
                const std::vector<std::string>& keys) {
  for (const auto& key : keys) {
    ASSERT_TRUE(index.Insert(key));
  }
}

std::vector<NodeVersionEntry> ScanForward(MasstreeIndex& index) {
  std::vector<NodeVersionEntry> entries;
  const auto count = index.Scan(
      std::string_view{}, std::optional<std::string_view>{},
      [](std::string_view) { return false; }, &entries);
  EXPECT_TRUE(count.has_value());
  return entries;
}

std::vector<NodeVersionEntry> ScanReverse(MasstreeIndex& index) {
  std::vector<NodeVersionEntry> entries;
  const std::string upper_bound(32, static_cast<char>(0xff));
  const auto count = index.ScanReverse(
      std::string_view{}, std::optional<std::string_view>(upper_bound),
      [](std::string_view) { return false; }, &entries);
  EXPECT_TRUE(count.has_value());
  return entries;
}

void ExpectLocatorsRedescend(MasstreeIndex& index,
                             const std::vector<NodeVersionEntry>& entries) {
  ASSERT_FALSE(entries.empty());
  for (const auto& entry : entries) {
    ASSERT_NE(entry.incarnation, 0U);
    const auto observed =
        index.ReadNodeVersion(entry.anchor_key, entry.layer_prefix_length);
    ASSERT_TRUE(observed.has_value())
        << "prefix=" << entry.layer_prefix_length
        << " anchor_size=" << entry.anchor_key.size();
    EXPECT_EQ(observed->incarnation, entry.incarnation);
    EXPECT_EQ(observed->version, entry.version);
  }
}

TEST(MasstreeIncarnationTest,
     ForwardAndReverseLocatorsCoverSingleAndMultipleLayers) {
  LineairDB::EpochFramework epoch(1000);
  epoch.Start();

  auto single_layer = MakeIndex(epoch);
  std::vector<std::string> short_keys;
  for (std::uint64_t i = 0; i < 64; ++i) {
    short_keys.push_back(OrderedKey(i));
  }
  InsertKeys(*single_layer, short_keys);
  ExpectLocatorsRedescend(*single_layer, ScanForward(*single_layer));
  ExpectLocatorsRedescend(*single_layer, ScanReverse(*single_layer));

  auto multiple_layers = MakeIndex(epoch);
  std::vector<std::string> long_keys;
  const std::string shared_prefix = "12345678abcdefgh";
  for (std::uint64_t i = 0; i < 64; ++i) {
    long_keys.push_back(shared_prefix + OrderedKey(i));
  }
  InsertKeys(*multiple_layers, long_keys);

  const auto forward = ScanForward(*multiple_layers);
  const auto reverse = ScanReverse(*multiple_layers);
  const auto has_deep_locator = [](const auto& entries) {
    return std::any_of(entries.begin(), entries.end(), [](const auto& entry) {
      return entry.layer_prefix_length >= 16;
    });
  };
  ASSERT_TRUE(has_deep_locator(forward));
  ASSERT_TRUE(has_deep_locator(reverse));
  ExpectLocatorsRedescend(*multiple_layers, forward);
  ExpectLocatorsRedescend(*multiple_layers, reverse);

  LineairDB::Index::MasstreeReleaseThreadEpoch();
}

TEST(MasstreeIncarnationTest, EmptyAndBoundaryOnlyScansHaveUsableLocators) {
  LineairDB::EpochFramework epoch(1000);
  epoch.Start();
  auto index = MakeIndex(epoch);

  auto empty_entries = ScanForward(*index);
  ASSERT_EQ(empty_entries.size(), 1U);
  ExpectLocatorsRedescend(*index, empty_entries);

  ASSERT_TRUE(index->Insert(OrderedKey(100)));
  std::vector<NodeVersionEntry> boundary_entries;
  const auto boundary = OrderedKey(50);
  const auto count = index->Scan(
      boundary, std::optional<std::string_view>(boundary),
      [](std::string_view) { return false; }, &boundary_entries);
  ASSERT_TRUE(count.has_value());
  EXPECT_EQ(*count, 0U);
  ExpectLocatorsRedescend(*index, boundary_entries);

  LineairDB::Index::MasstreeReleaseThreadEpoch();
}

TEST(MasstreeIncarnationTest, MalformedLayerPrefixesFailClosed) {
  LineairDB::EpochFramework epoch(1000);
  epoch.Start();
  auto index = MakeIndex(epoch);
  const std::string key = "12345678abcdefgh";
  ASSERT_TRUE(index->Insert(key));

  EXPECT_FALSE(index->ReadNodeVersion(key, 1).has_value());
  EXPECT_FALSE(index->ReadNodeVersion(key, 8).has_value());
  EXPECT_FALSE(index->ReadNodeVersion(key.substr(0, 8), 8).has_value());
  EXPECT_FALSE(index->ReadNodeVersion(key, 24).has_value());

  LineairDB::Index::MasstreeReleaseThreadEpoch();
}

TEST(MasstreeIncarnationTest, StructuralUpdateCarriesLeafLocator) {
  LineairDB::EpochFramework epoch(1000);
  epoch.Start();
  auto index = MakeIndex(epoch);
  const std::string shared_prefix = "12345678abcdefgh";
  ASSERT_TRUE(index->Insert(shared_prefix + OrderedKey(1)));

  LineairDB::Index::NodeVersionUpdate update;
  const auto inserted_key = shared_prefix + OrderedKey(2);
  ASSERT_TRUE(index->Insert(inserted_key, &update));
  ASSERT_TRUE(update.valid);
  EXPECT_EQ(update.anchor_key, inserted_key);
  EXPECT_GE(update.layer_prefix_length, 16U);
  EXPECT_NE(update.incarnation, 0U);

  const auto observed =
      index->ReadNodeVersion(update.anchor_key,
                             update.layer_prefix_length);
  ASSERT_TRUE(observed.has_value());
  EXPECT_EQ(observed->incarnation, update.incarnation);
  EXPECT_EQ(observed->version, update.new_version);

  LineairDB::Index::MasstreeReleaseThreadEpoch();
}

TEST(MasstreeIncarnationTest,
     ReclaimedLeafAddressReceivesDifferentIncarnation) {
  LineairDB::EpochFramework epoch(1000);
  epoch.Start();
  auto index = MakeIndex(epoch);

  std::vector<std::string> original_keys;
  for (std::uint64_t i = 0; i < 96; ++i) {
    original_keys.push_back(OrderedKey(i));
  }
  InsertKeys(*index, original_keys);

  const auto before = ScanForward(*index);
  ASSERT_GT(before.size(), 1U);
  const auto retired = before.back();
  ASSERT_NE(retired.node_ptr, before.front().node_ptr);

  std::vector<std::string> retired_leaf_keys;
  for (const auto& key : original_keys) {
    const auto observed = index->ReadNodeVersion(key, 0);
    ASSERT_TRUE(observed.has_value());
    if (observed->incarnation == retired.incarnation) {
      retired_leaf_keys.push_back(key);
    }
  }
  ASSERT_FALSE(retired_leaf_keys.empty());

  for (const auto& key : retired_leaf_keys) {
    auto* item = index->Get(key);
    ASSERT_NE(item, nullptr);
    ASSERT_TRUE(index->Purge(key, item));
  }

  LineairDB::Index::MasstreeReleaseThreadEpoch();
  for (int i = 0; i < 4; ++i) {
    LineairDB::Index::MasstreeAdvanceEpoch();
    LineairDB::Index::MasstreeReleaseThreadEpoch();
  }

  const auto after_reclaim = ScanForward(*index);
  EXPECT_TRUE(std::none_of(
      after_reclaim.begin(), after_reclaim.end(), [&](const auto& entry) {
        return entry.node_ptr == retired.node_ptr;
      }));

  std::optional<NodeVersionEntry> reused;
  for (std::uint64_t i = 1000; i < 1512 && !reused.has_value(); ++i) {
    ASSERT_TRUE(index->Insert(OrderedKey(i)));
    const auto entries = ScanForward(*index);
    const auto it =
        std::find_if(entries.begin(), entries.end(), [&](const auto& entry) {
          return entry.node_ptr == retired.node_ptr;
        });
    if (it != entries.end()) reused = *it;
  }

  ASSERT_TRUE(reused.has_value())
      << "the same-size Masstree pool did not reuse the reclaimed leaf";
  EXPECT_NE(reused->incarnation, retired.incarnation);

  auto equal_version_stale_witness = retired;
  equal_version_stale_witness.anchor_key = reused->anchor_key;
  equal_version_stale_witness.layer_prefix_length =
      reused->layer_prefix_length;
  equal_version_stale_witness.version = reused->version;
  const auto current =
      index->ReadNodeVersion(equal_version_stale_witness.anchor_key,
                             equal_version_stale_witness.layer_prefix_length);
  ASSERT_TRUE(current.has_value());
  EXPECT_EQ(current->version, equal_version_stale_witness.version);
  EXPECT_NE(current->incarnation,
            equal_version_stale_witness.incarnation)
      << "incarnation must reject an equal-version stale witness";

  LineairDB::Index::MasstreeReleaseThreadEpoch();
}

TEST(MasstreeIncarnationTest, CounterExhaustionFailsClosed) {
  LineairDB::EpochFramework epoch(1000);
  epoch.Start();
  constexpr auto kLastIncarnation =
      (std::uint64_t{1} << 63) - 1;
  const auto saved =
      LineairDB::Index::MasstreeSetNextLeafIncarnationForTesting(
          kLastIncarnation);

  auto last_index = MakeIndex(epoch);
  const auto last = last_index->ReadNodeVersion("", 0);
  auto exhausted_index = MakeIndex(epoch);
  const auto exhausted = exhausted_index->ReadNodeVersion("", 0);

  LineairDB::Index::MasstreeSetNextLeafIncarnationForTesting(saved);
  ASSERT_TRUE(last.has_value());
  EXPECT_EQ(last->incarnation, kLastIncarnation);
  EXPECT_FALSE(exhausted.has_value());

  LineairDB::Index::MasstreeReleaseThreadEpoch();
}

}  // namespace
