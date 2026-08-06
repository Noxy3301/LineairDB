#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/file.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "recovery/flush_trace.h"

namespace {

using LineairDB::Recovery::FlushTrace;

// The one data row of "<prefix>_meta.csv".
struct Meta {
  uint64_t generation = 0;
  uint64_t groups = 0;
  uint64_t closes = 0;
  uint64_t commits = 0;
};

// A fresh, unique directory under the system temp dir, removed on scope
// exit. Each caller gets its own directory so the two tests below cannot see
// each other's files.
class ScopedTraceDir {
 public:
  explicit ScopedTraceDir(const char* tag) {
    std::string pattern = (std::filesystem::temp_directory_path() /
                           (std::string("lineairdb_flush_trace_") + tag +
                            "_XXXXXX"))
                              .string();
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    EXPECT_NE(::mkdtemp(buffer.data()), nullptr);
    path_ = buffer.data();
  }
  ~ScopedTraceDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }
  std::string prefix() const { return path_ + "/trace"; }

 private:
  std::string path_;
};

Meta ReadMeta(const std::string& prefix) {
  Meta meta;
  std::ifstream file(prefix + "_meta.csv");
  EXPECT_TRUE(file.is_open()) << prefix + "_meta.csv";
  std::string header;
  std::getline(file, header);
  std::string line;
  EXPECT_TRUE(static_cast<bool>(std::getline(file, line)));
  std::istringstream row(line);
  std::string field;
  std::getline(row, field, ',');
  meta.generation = std::stoull(field);
  std::getline(row, field, ',');
  meta.groups = std::stoull(field);
  std::getline(row, field, ',');  // group_drops
  std::getline(row, field, ',');
  meta.closes = std::stoull(field);
  std::getline(row, field, ',');  // close_drops
  std::getline(row, field, ',');
  meta.commits = std::stoull(field);
  return meta;
}

std::string ReadWholeFile(const std::string& path) {
  std::ifstream file(path, std::ios::binary);
  std::ostringstream contents;
  contents << file.rdbuf();
  return contents.str();
}

// Records two flush groups and one epoch closure, then dumps them and exits.
// Runs inside a death-test child only: FlushTrace reads its enable switch at
// the first Instance() call, so the parent must never construct it.
void RecordAndDump() {
  auto& trace = FlushTrace::Instance();
  ASSERT_TRUE(trace.Enabled());

  trace.GroupCollectBegin(/*durable_before=*/0);
  trace.GroupCollectEnd();
  trace.GroupEncode(/*begin=*/1, /*end=*/2, /*bytes=*/128, /*epochs=*/1);
  trace.GroupWrite(/*begin=*/2, /*end=*/3);
  trace.GroupSync(/*begin=*/3, /*end=*/4);
  trace.GroupPublish(/*target=*/1, /*enter=*/4, /*exit=*/5);

  trace.GroupCollectBegin(/*durable_before=*/1);
  trace.GroupCollectEnd();
  trace.GroupEncode(/*begin=*/5, /*end=*/6, /*bytes=*/64, /*epochs=*/1);
  trace.GroupWrite(/*begin=*/6, /*end=*/7);
  trace.GroupSync(/*begin=*/7, /*end=*/8);
  trace.GroupPublish(/*target=*/2, /*enter=*/8, /*exit=*/9);

  trace.EpochClosed(/*closed=*/2, /*enter=*/0, /*exit=*/1);

  trace.Dump();
  _exit(0);
}

}  // namespace

TEST(FlushTraceTest, DumpWritesAManifestNamingExistingDataFiles) {
  ScopedTraceDir dir("dump");
  const std::string prefix = dir.prefix();
  EXPECT_EXIT(
      {
        ASSERT_EQ(::setenv("LINEAIRDB_FLUSH_TRACE", prefix.c_str(), 1), 0);
        RecordAndDump();
      },
      ::testing::ExitedWithCode(0), "");

  const Meta meta = ReadMeta(prefix);
  EXPECT_EQ(meta.groups, 2u);
  EXPECT_EQ(meta.closes, 1u);

  const std::string stem = prefix + "_g" + std::to_string(meta.generation);
  EXPECT_TRUE(std::filesystem::exists(stem + "_groups.csv"));
  EXPECT_TRUE(std::filesystem::exists(stem + "_closes.csv"));
  EXPECT_TRUE(std::filesystem::exists(stem + "_commits.csv"));
}

TEST(FlushTraceTest, ASecondDumpDoesNotReplaceTheFirstGeneration) {
  ScopedTraceDir dir("redump");
  const std::string prefix = dir.prefix();
  EXPECT_EXIT(
      {
        ASSERT_EQ(::setenv("LINEAIRDB_FLUSH_TRACE", prefix.c_str(), 1), 0);
        RecordAndDump();
      },
      ::testing::ExitedWithCode(0), "");

  const Meta first = ReadMeta(prefix);
  const std::string first_stem =
      prefix + "_g" + std::to_string(first.generation);
  const std::string first_groups = ReadWholeFile(first_stem + "_groups.csv");
  ASSERT_FALSE(first_groups.empty());

  // A second process, same prefix: its own FlushTrace claims the next
  // generation the filesystem does not already hold.
  EXPECT_EXIT(
      {
        ASSERT_EQ(::setenv("LINEAIRDB_FLUSH_TRACE", prefix.c_str(), 1), 0);
        RecordAndDump();
      },
      ::testing::ExitedWithCode(0), "");

  const Meta second = ReadMeta(prefix);
  EXPECT_GT(second.generation, first.generation);

  // The first generation's files are untouched by the second dump.
  EXPECT_TRUE(std::filesystem::exists(first_stem + "_groups.csv"));
  EXPECT_EQ(ReadWholeFile(first_stem + "_groups.csv"), first_groups);
}

TEST(FlushTraceTest, ALockedPrefixLeavesTracingDisabled) {
  ScopedTraceDir dir("locked");
  const std::string prefix = dir.prefix();

  // Stand in for another process already holding the prefix: the parent
  // takes the lock itself but still never constructs FlushTrace.
  const int lock_fd =
      ::open((prefix + ".lock").c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
  ASSERT_GE(lock_fd, 0);
  ASSERT_EQ(::flock(lock_fd, LOCK_EX | LOCK_NB), 0);

  EXPECT_EXIT(
      {
        ASSERT_EQ(::setenv("LINEAIRDB_FLUSH_TRACE", prefix.c_str(), 1), 0);
        ASSERT_FALSE(FlushTrace::Instance().Enabled());
        _exit(0);
      },
      ::testing::ExitedWithCode(0), "");

  EXPECT_FALSE(std::filesystem::exists(prefix + "_meta.csv"));
  ::close(lock_fd);
}

TEST(FlushTraceTest, AnUnusableGenerationScanReleasesTheLock) {
  ScopedTraceDir dir("unusable");
  const std::string prefix = dir.prefix();

  // A generation file that is a symlink to itself makes stat fail with
  // ELOOP: not ENOENT, so the scan reports the prefix unusable while the
  // lock file itself stays lockable.
  const std::string g0 = prefix + "_g0_groups.csv";
  ASSERT_EQ(::symlink(g0.c_str(), g0.c_str()), 0);

  EXPECT_EXIT(
      {
        ASSERT_EQ(::setenv("LINEAIRDB_FLUSH_TRACE", prefix.c_str(), 1), 0);
        if (FlushTrace::Instance().Enabled()) _exit(1);
        // flock is per open file description, so a leaked construction-time
        // lock would still block this second descriptor in the same process.
        const int fd = ::open((prefix + ".lock").c_str(), O_RDWR);
        if (fd < 0) _exit(2);
        if (::flock(fd, LOCK_EX | LOCK_NB) != 0) _exit(3);
        _exit(0);
      },
      ::testing::ExitedWithCode(0), "");
}
