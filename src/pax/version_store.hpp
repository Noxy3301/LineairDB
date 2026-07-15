#ifndef LINEAIRDB_PAX_VERSION_STORE_HPP
#define LINEAIRDB_PAX_VERSION_STORE_HPP

#include <lineairdb/pax_store.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace LineairDB {
namespace Pax {

/**
 * @brief Undo-style version store for columnar read views.
 *
 * @details While at least one read view is active (capture_active > 0), every
 * PAX install publishes the replaced row image before its first strip-cell
 * or visibility-bit mutation; a first install publishes an empty
 * was_visible=false entry. An install that cannot publish (byte budget
 * exceeded, or no commit epoch) poisons the active generation instead, and
 * every result produced under a poisoned generation is discarded. A reader
 * with cut epoch E resolves a slot to the before-image of the oldest entry
 * whose writer epoch exceeds E and reads the strip in place when no entry
 * qualifies. With no read view active the writer side pays one atomic load
 * per installed row.
 */
class VersionStore {
 public:
  // The stored form is the public reader-side type; readers receive copies.
  using Entry = UndoEntry;

  /**
   * @brief Per-group undo state: the captured entries and their publish
   * counter.
   */
  struct GroupUndo {
    mutable std::mutex m;
    // Monotonic publish counter, incremented after the entry is appended
    // and before the writer's first strip mutation; readers sample it to
    // detect concurrent writers.
    std::atomic<uint64_t> capture_count{0};
    std::unordered_map<uint32_t, std::vector<Entry>> entries;
  };

  /**
   * @brief Returns the process-wide store instance.
   */
  static VersionStore& Global();

  // Writer side ------------------------------------------------------

  /**
   * @brief Returns whether at least one read view is active.
   */
  bool CaptureActive() const {
    return capture_active_.load(std::memory_order_seq_cst) > 0;
  }

  /**
   * @brief Publishes a before-image for (group, slot).
   *
   * @details Must run before the install's first strip-cell or
   * visibility-bit mutation. A first install into a fresh slot passes
   * was_visible=false and an empty row.
   */
  void Capture(PaxGroup* group, uint32_t slot, uint32_t writer_epoch,
               bool was_visible, std::string old_row);

  // Reader side ------------------------------------------------------

  /**
   * @brief Identifies one active read view's capture registration.
   */
  struct ReadViewToken {
    uint64_t id = 0;
    bool valid = false;
  };

  /**
   * @brief Arms capturing; the caller performs the epoch fence itself.
   */
  ReadViewToken BeginCapture();

  /**
   * @brief Releases one registration; call exactly once per valid token.
   *
   * @details The release of the last active registration clears every
   * group's entries and resets the poison flag. Token ids are diagnostic;
   * a double or stale release is not detected.
   */
  void EndCapture(const ReadViewToken& token);

  /**
   * @brief Returns whether the generation was poisoned while this read view
   * was active; a poisoned read view's result must be discarded.
   */
  bool Poisoned(const ReadViewToken& token) const;

  /**
   * @brief Fails every active read view and rejects new ones until the last
   * active read view releases.
   *
   * @details Callers must publish the poison before mutating any cell the
   * failed capture should have covered. A poison racing the last release
   * may land on the next generation, whose results are then discarded;
   * both orderings fail closed.
   */
  void PoisonActiveGeneration(const char* reason);

  /**
   * @brief Returns the capture_count of the group's undo map, 0 if the
   * group has none.
   */
  uint64_t GroupCaptureCount(const PaxGroup* group) const;

  /**
   * @brief Copies the entries for (group, slot); empty if none.
   *
   * @details Copies keep callers immune to concurrent vector reallocation.
   */
  std::vector<Entry> EntriesFor(const PaxGroup* group, uint32_t slot) const;

  /**
   * @brief Copies the group's whole slot->entries map in one locking pass.
   */
  std::unordered_map<uint32_t, std::vector<Entry>> GroupEntries(
      const PaxGroup* group) const;

 private:
  // seq_cst on both sides is load-bearing for the fence proof; do not
  // weaken.
  std::atomic<uint64_t> capture_active_{0};
  std::atomic<uint64_t> captured_bytes_{0};
  // Covers the whole active generation; resets when the last read view
  // releases.
  std::atomic<bool> poisoned_{false};

  // shared: captures; exclusive: the zero-transition clear. Group
  // pointers serve as map keys only and are never dereferenced by the
  // store.
  mutable std::shared_mutex registry_mutex_;
  mutable std::mutex groups_mutex_;
  std::unordered_map<const PaxGroup*, std::unique_ptr<GroupUndo>> groups_;

  uint64_t next_view_id_ = 1;
  uint64_t byte_budget_;

  VersionStore();
  GroupUndo* GetOrCreateGroupUndo(PaxGroup* group);
  const GroupUndo* FindGroupUndo(const PaxGroup* group) const;
  void ClearAllLocked();
};

/**
 * @brief Thread-local commit epoch of the install region in progress; zero
 * outside one (e.g. recovery replay).
 *
 * @details The capture hook tags entries with it and poisons the active
 * generation on a zero-epoch install.
 */
struct CurrentCommitEpoch {
  static uint32_t& Get();
};

/**
 * @brief RAII bracket for an install region; restores the previous value so
 * nested installs cannot leak a tag past their scope.
 */
class ScopedCommitEpoch {
 public:
  explicit ScopedCommitEpoch(uint32_t epoch)
      : previous_(CurrentCommitEpoch::Get()) {
    CurrentCommitEpoch::Get() = epoch;
  }
  ~ScopedCommitEpoch() { CurrentCommitEpoch::Get() = previous_; }
  ScopedCommitEpoch(const ScopedCommitEpoch&) = delete;
  ScopedCommitEpoch& operator=(const ScopedCommitEpoch&) = delete;

 private:
  const uint32_t previous_;
};

}  // namespace Pax
}  // namespace LineairDB

#endif  // LINEAIRDB_PAX_VERSION_STORE_HPP
