#include "types/packed_primary_keys.hpp"

#include <algorithm>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"

namespace {

using LineairDB::PackedPrimaryKeys;
using LineairDB::PackedPrimaryKeysView;

auto LowerBound(std::vector<std::string>& keys, std::string_view key) {
  auto cmp = [](const std::string& a, std::string_view b) { return a < b; };
  return std::lower_bound(keys.begin(), keys.end(), key, cmp);
}

auto LowerBound(const std::vector<std::string>& keys, std::string_view key) {
  auto cmp = [](const std::string& a, std::string_view b) { return a < b; };
  return std::lower_bound(keys.begin(), keys.end(), key, cmp);
}

bool OracleInsert(std::vector<std::string>& keys, std::string_view key) {
  auto it = LowerBound(keys, key);
  if (it != keys.end() && std::string_view(*it) == key) return false;
  keys.emplace(it, key);
  return true;
}

bool OracleErase(std::vector<std::string>& keys, std::string_view key) {
  auto it = LowerBound(keys, key);
  if (it == keys.end() || std::string_view(*it) != key) return false;
  keys.erase(it);
  return true;
}

std::vector<std::string> ToVector(PackedPrimaryKeysView view) {
  std::vector<std::string> values;
  for (std::string_view value : view) {
    values.emplace_back(value);
  }
  return values;
}

void ExpectMatchesOracle(const PackedPrimaryKeys::Ptr& primary_keys,
                         const std::vector<std::string>& oracle,
                         const std::vector<std::string>& probes) {
  const PackedPrimaryKeysView view(primary_keys);
  ASSERT_EQ(view.size(), oracle.size());
  ASSERT_EQ(view.empty(), oracle.empty());

  size_t index = 0;
  for (std::string_view value : view) {
    ASSERT_LT(index, oracle.size());
    EXPECT_EQ(value, std::string_view(oracle[index])) << "index " << index;
    ++index;
  }
  EXPECT_EQ(index, oracle.size());

  for (const auto& key : probes) {
    const auto oracle_it = LowerBound(oracle, key);
    const bool oracle_contains =
        oracle_it != oracle.end() && std::string_view(*oracle_it) == key;
    const auto it = view.lower_bound(key);

    EXPECT_EQ(view.contains(key), oracle_contains) << "probe " << key;
    if (oracle_it == oracle.end()) {
      EXPECT_EQ(it, view.end()) << "probe " << key;
    } else {
      ASSERT_NE(it, view.end()) << "probe " << key;
      EXPECT_EQ(*it, std::string_view(*oracle_it)) << "probe " << key;
    }
  }
}

std::vector<std::string> ProbeKeys(const std::vector<std::string>& oracle,
                                   const std::vector<std::string>& universe) {
  std::vector<std::string> probes = {
      "",
      "a",
      "b",
      "aa",
      "zz",
      std::string(127, 'm'),
      std::string(128, 'm'),
      std::string(255, 'z'),
  };
  for (const auto& key : oracle) probes.push_back(key);
  for (const auto& key : universe) probes.push_back(key);
  return probes;
}

std::vector<std::string> BuildKeyUniverse() {
  // Short overlapping keys ("a", "ab", "b", ...) exercise prefix and
  // lexicographic ordering edges. The 127/128/129/300-length keys exercise
  // the varint length-prefix 1->2 byte boundary.
  std::vector<std::string> keys = {
      "",
      "a",
      "b",
      "c",
      "aa",
      "ab",
      "ba",
      "bb",
      "abc",
      "cab",
      "zz",
      std::string(127, 'x'),
      std::string(128, 'x'),
      std::string(129, 'x'),
      std::string(300, 'y'),
  };

  const char alphabet[] = {'a', 'b', 'c'};
  for (char a : alphabet) {
    for (char b : alphabet) {
      keys.emplace_back(std::string({a, b}));
    }
  }
  std::sort(keys.begin(), keys.end());
  keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  return keys;
}

}  // namespace

TEST(PackedPrimaryKeysTest, DedupInsertAndEraseMissingReturnSameInstance) {
  const std::vector<std::string> seed = {"a", "b", std::string(128, 'x')};
  auto primary_keys = PackedPrimaryKeys::FromSortedDeduped(seed);

  const auto duplicate = PackedPrimaryKeys::Insert(primary_keys, "b");
  EXPECT_EQ(duplicate.get(), primary_keys.get());
  EXPECT_TRUE(PackedPrimaryKeysView(duplicate)
                  .equals(PackedPrimaryKeysView(primary_keys)));

  const auto missing = PackedPrimaryKeys::Erase(primary_keys, "missing");
  EXPECT_EQ(missing.get(), primary_keys.get());
  EXPECT_TRUE(PackedPrimaryKeysView(missing)
                  .equals(PackedPrimaryKeysView(primary_keys)));
}

TEST(PackedPrimaryKeysTest, SortedFactoryEqualsIncrementalInsertions) {
  std::vector<std::string> sorted = {
      "",
      "a",
      "ab",
      "b",
      "zz",
      std::string(128, 'x'),
      std::string(300, 'y'),
  };
  std::sort(sorted.begin(), sorted.end());
  sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());

  auto from_factory = PackedPrimaryKeys::FromSortedDeduped(sorted);
  auto incremental = PackedPrimaryKeys::FromSortedDeduped({});
  for (const auto& key : sorted) {
    incremental = PackedPrimaryKeys::Insert(incremental, key);
  }

  EXPECT_TRUE(PackedPrimaryKeysView(from_factory)
                  .equals(PackedPrimaryKeysView(incremental)));
  EXPECT_EQ(ToVector(PackedPrimaryKeysView(incremental)), sorted);
}

TEST(PackedPrimaryKeysTest, RandomizedOperationsLockstepWithVectorOracle) {
  const auto universe = BuildKeyUniverse();
  auto primary_keys = PackedPrimaryKeys::FromSortedDeduped({});
  std::vector<std::string> oracle;
  std::mt19937 rng(0x51b10b);
  std::uniform_int_distribution<size_t> key_dist(0, universe.size() - 1);
  std::bernoulli_distribution insert_dist(0.55);

  for (size_t step = 0; step != 5000; ++step) {
    const std::string& key = universe[key_dist(rng)];
    if (insert_dist(rng)) {
      const bool changed = OracleInsert(oracle, key);
      const auto previous = primary_keys;
      primary_keys = PackedPrimaryKeys::Insert(primary_keys, key);
      if (!changed) {
        EXPECT_EQ(primary_keys.get(), previous.get()) << "step " << step;
      }
    } else {
      const bool changed = OracleErase(oracle, key);
      const auto previous = primary_keys;
      primary_keys = PackedPrimaryKeys::Erase(primary_keys, key);
      if (!changed) {
        EXPECT_EQ(primary_keys.get(), previous.get()) << "step " << step;
      }
    }

    ExpectMatchesOracle(primary_keys, oracle, ProbeKeys(oracle, universe));
  }
}
