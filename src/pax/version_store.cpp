#include "pax/version_store.hpp"

#include <cstdlib>

#include "util/logger.hpp"

namespace LineairDB {
namespace Pax {

namespace {
/**
 * @brief Byte budget for captured before-images.
 *
 * @details Exceeding it poisons every active read view instead of growing
 * writer-side memory without bound.
 */
uint64_t ByteBudgetFromEnv() {
  constexpr uint64_t kDefault = 256ull << 20;
  const char* v = std::getenv("LINEAIRDB_VERSION_STORE_BUDGET_BYTES");
  if (v == nullptr) return kDefault;
  const long long parsed = std::strtoll(v, nullptr, 10);
  return parsed > 0 ? static_cast<uint64_t>(parsed) : kDefault;
}
}  // namespace

VersionStore::VersionStore() : byte_budget_(ByteBudgetFromEnv()) {}

VersionStore& VersionStore::Global() {
  static VersionStore instance;
  return instance;
}

VersionStore::GroupUndo* VersionStore::GetOrCreateGroupUndo(PaxGroup* group) {
  std::lock_guard<std::mutex> lk(groups_mutex_);
  auto& slot = groups_[group];
  if (!slot) slot = std::make_unique<GroupUndo>();
  return slot.get();
}

const VersionStore::GroupUndo* VersionStore::FindGroupUndo(
    const PaxGroup* group) const {
  std::lock_guard<std::mutex> lk(groups_mutex_);
  auto it = groups_.find(group);
  return it == groups_.end() ? nullptr : it->second.get();
}

void VersionStore::Capture(PaxGroup* group, uint32_t slot,
                           uint32_t writer_epoch, bool was_visible,
                           std::string old_row) {
  // Shared lock: keeps the zero-transition clear (exclusive) from
  // destroying entry vectors under an in-flight capture. A capture racing
  // the last EndCapture may still append; the entry is cleared by that
  // same exclusive pass or ignored by epoch filtering.
  std::shared_lock<std::shared_mutex> lk(registry_mutex_);
  if (capture_active_.load(std::memory_order_seq_cst) == 0) return;

  const uint64_t added = old_row.size() + sizeof(Entry);
  if (captured_bytes_.fetch_add(added, std::memory_order_relaxed) + added >
      byte_budget_) {
    poisoned_.store(true, std::memory_order_seq_cst);
    SPDLOG_WARN(
        "PAX version store byte budget exceeded; the active capture "
        "generation is poisoned and its results will be discarded");
    return;
  }

  GroupUndo* undo = GetOrCreateGroupUndo(group);
  {
    std::lock_guard<std::mutex> glk(undo->m);
    undo->entries[slot].push_back(
        Entry{writer_epoch, was_visible, std::move(old_row)});
  }
  // Publish after the append and before the caller mutates any strip
  // cell: a reader whose count recheck still passes finished its in-place
  // reads before this writer's first cell write.
  undo->capture_count.fetch_add(1, std::memory_order_release);
}

VersionStore::ReadViewToken VersionStore::BeginCapture() {
  std::unique_lock<std::shared_mutex> lk(registry_mutex_);
  ReadViewToken token;
  // A poisoned generation may contain mutations with no entry and no
  // count advance; a joining read view could trust a window it must not.
  // Reject until the last member releases.
  if (capture_active_.load(std::memory_order_seq_cst) > 0 &&
      poisoned_.load(std::memory_order_seq_cst)) {
    return token;
  }
  token.id = next_view_id_++;
  token.valid = true;
  // seq_cst is load-bearing for the read view fence; do not weaken.
  capture_active_.fetch_add(1, std::memory_order_seq_cst);
  return token;
}

void VersionStore::EndCapture(const ReadViewToken& token) {
  if (!token.valid) return;
  std::unique_lock<std::shared_mutex> lk(registry_mutex_);
  const auto remaining =
      capture_active_.fetch_sub(1, std::memory_order_seq_cst) - 1;
  if (remaining == 0) ClearAllLocked();
}

bool VersionStore::Poisoned(const ReadViewToken& token) const {
  (void)token;
  return poisoned_.load(std::memory_order_seq_cst);
}

void VersionStore::PoisonActiveGeneration(const char* reason) {
  poisoned_.store(true, std::memory_order_seq_cst);
  SPDLOG_WARN("PAX version store poisoned: {}", reason);
}

uint64_t VersionStore::GroupCaptureCount(const PaxGroup* group) const {
  const GroupUndo* undo = FindGroupUndo(group);
  return undo == nullptr ? 0
                         : undo->capture_count.load(std::memory_order_acquire);
}

std::vector<VersionStore::Entry> VersionStore::EntriesFor(
    const PaxGroup* group, uint32_t slot) const {
  const GroupUndo* undo = FindGroupUndo(group);
  if (undo == nullptr) return {};
  std::lock_guard<std::mutex> glk(undo->m);
  auto it = undo->entries.find(slot);
  if (it == undo->entries.end()) return {};
  return it->second;
}

std::unordered_map<uint32_t, std::vector<VersionStore::Entry>>
VersionStore::GroupEntries(const PaxGroup* group) const {
  const GroupUndo* undo = FindGroupUndo(group);
  if (undo == nullptr) return {};
  std::lock_guard<std::mutex> glk(undo->m);
  return undo->entries;
}

void VersionStore::ClearAllLocked() {
  std::lock_guard<std::mutex> lk(groups_mutex_);
  for (auto& [group, undo] : groups_) {
    std::lock_guard<std::mutex> glk(undo->m);
    undo->entries.clear();
    // The reset cannot alias two generations: counter comparisons happen
    // between samples under one active read view and this clear runs only
    // while none is active. Without it a once-captured group would lose
    // the bulk read path for the rest of the process lifetime.
    undo->capture_count.store(0, std::memory_order_release);
  }
  captured_bytes_.store(0, std::memory_order_relaxed);
  poisoned_.store(false, std::memory_order_seq_cst);
}

uint32_t& CurrentCommitEpoch::Get() {
  thread_local uint32_t epoch = 0;
  return epoch;
}

uint64_t UndoGroupCaptureCount(const PaxGroup* group) {
  return VersionStore::Global().GroupCaptureCount(group);
}

std::unordered_map<uint32_t, std::vector<UndoEntry>> UndoGroupEntries(
    const PaxGroup* group) {
  return VersionStore::Global().GroupEntries(group);
}

std::vector<UndoEntry> UndoSlotEntries(const PaxGroup* group,
                                       uint32_t slot) {
  return VersionStore::Global().EntriesFor(group, slot);
}

}  // namespace Pax
}  // namespace LineairDB
