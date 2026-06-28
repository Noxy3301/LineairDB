#ifndef LINEAIRDB_PACKED_PRIMARY_KEYS_HPP
#define LINEAIRDB_PACKED_PRIMARY_KEYS_HPP

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace LineairDB {

class PackedPrimaryKeysView;

/** @brief Immutable, sorted, deduplicated, length-prefixed primary-key list
 * held as `std::shared_ptr<const PackedPrimaryKeys>` in one allocation. */
struct PackedPrimaryKeys {
  using Ptr = std::shared_ptr<const PackedPrimaryKeys>;

  uint32_t count;
  uint32_t bytes;

  /**
   * @brief Builds an immutable primary-key list from already sorted,
   * deduplicated keys.
   * @param keys Primary keys in strict lexicographic order.
   * @return Primary-key list containing exactly `keys`.
   * @throws std::length_error if the count or payload byte length exceeds
   * `uint32_t`.
   */
  static Ptr FromSortedDeduped(const std::vector<std::string>& keys) {
    size_t payload_bytes = 0;
    for (const auto& key : keys) {
      payload_bytes += EncodedRecordSize(key);
      CheckFitsUint32(payload_bytes, "packed primary-key list bytes");
    }

    auto packed = AllocateMutable(
        CheckFitsUint32(keys.size(), "packed primary-key list count"),
        static_cast<uint32_t>(payload_bytes));
    char* out = packed->MutableRecords();
    for (const auto& key : keys) {
      out = WriteRecord(out, key);
    }
    assert(out == packed->MutableRecords() + packed->bytes);
    return packed;
  }

  /**
   * @brief Inserts a key while preserving sorted, deduplicated invariants.
   * @param keys Existing immutable primary-key list, or null for an empty
   * list.
   * @param key Primary key to insert.
   * @return The same `shared_ptr` when `key` is already present; otherwise a
   * new immutable primary-key list containing `key`.
   * @throws std::length_error if the updated count or payload byte length
   * exceeds `uint32_t`.
   */
  static Ptr Insert(const Ptr& keys, std::string_view key) {
    if (!keys) {
      return FromOne(key);
    }

    const char* const src_begin = keys->Records();
    const char* const src_end = src_begin + keys->bytes;
    const char* insert_pos = src_end;

    for (const char* cursor = src_begin; cursor != src_end;) {
      const Record record = DecodeRecord(cursor, src_end);
      const std::string_view value(record.value, record.length);
      if (value == key) return keys;
      if (!(value < key)) {
        insert_pos = record.start;
        break;
      }
      cursor = record.next;
    }

    const size_t added_bytes = EncodedRecordSize(key);
    const size_t new_bytes = static_cast<size_t>(keys->bytes) + added_bytes;
    auto next = AllocateMutable(
        CheckFitsUint32(static_cast<size_t>(keys->count) + 1,
                        "packed primary-key list count"),
        CheckFitsUint32(new_bytes, "packed primary-key list bytes"));

    char* out = next->MutableRecords();
    const size_t prefix_bytes = static_cast<size_t>(insert_pos - src_begin);
    if (prefix_bytes != 0) {
      std::memcpy(out, src_begin, prefix_bytes);
      out += prefix_bytes;
    }
    out = WriteRecord(out, key);
    const size_t suffix_bytes = static_cast<size_t>(src_end - insert_pos);
    if (suffix_bytes != 0) {
      std::memcpy(out, insert_pos, suffix_bytes);
      out += suffix_bytes;
    }
    assert(out == next->MutableRecords() + next->bytes);
    return next;
  }

  /**
   * @brief Erases a key while preserving sorted, deduplicated invariants.
   * @param keys Existing immutable primary-key list, or null for an empty
   * list.
   * @param key Primary key to erase.
   * @return The same `shared_ptr` when `keys` is null, empty, or does not
   * contain `key`; otherwise a new immutable primary-key list without `key`.
   */
  static Ptr Erase(const Ptr& keys, std::string_view key) {
    if (!keys || keys->count == 0) return keys;

    const char* const src_begin = keys->Records();
    const char* const src_end = src_begin + keys->bytes;

    for (const char* cursor = src_begin; cursor != src_end;) {
      const Record record = DecodeRecord(cursor, src_end);
      const std::string_view value(record.value, record.length);
      if (value == key) {
        const size_t removed_bytes =
            static_cast<size_t>(record.next - record.start);
        const size_t new_bytes =
            static_cast<size_t>(keys->bytes) - removed_bytes;
        auto next = AllocateMutable(
            CheckFitsUint32(static_cast<size_t>(keys->count) - 1,
                            "packed primary-key list count"),
            static_cast<uint32_t>(new_bytes));

        char* out = next->MutableRecords();
        const size_t prefix_bytes =
            static_cast<size_t>(record.start - src_begin);
        if (prefix_bytes != 0) {
          std::memcpy(out, src_begin, prefix_bytes);
          out += prefix_bytes;
        }
        const size_t suffix_bytes =
            static_cast<size_t>(src_end - record.next);
        if (suffix_bytes != 0) {
          std::memcpy(out, record.next, suffix_bytes);
          out += suffix_bytes;
        }
        assert(out == next->MutableRecords() + next->bytes);
        return next;
      }
      if (!(value < key)) return keys;
      cursor = record.next;
    }

    return keys;
  }

  /**
   * @brief Returns the encoded record payload after the fixed header.
   * @return Pointer to `bytes` length-prefixed primary-key records.
   */
  const char* Records() const {
    return reinterpret_cast<const char*>(this) + sizeof(PackedPrimaryKeys);
  }

 private:
  friend class PackedPrimaryKeysView;

  struct Record {
    const char* start;
    const char* value;
    const char* next;
    size_t length;
  };

  struct Deleter {
    void operator()(PackedPrimaryKeys* keys) const noexcept {
      keys->~PackedPrimaryKeys();
      delete[] reinterpret_cast<std::byte*>(keys);
    }
  };

  using MutablePtr = std::shared_ptr<PackedPrimaryKeys>;

  PackedPrimaryKeys(uint32_t record_count, uint32_t payload_bytes)
      : count(record_count), bytes(payload_bytes) {}

  static MutablePtr AllocateMutable(uint32_t record_count,
                                    uint32_t payload_bytes) {
    const size_t allocation_size =
        sizeof(PackedPrimaryKeys) + static_cast<size_t>(payload_bytes);
    std::byte* raw = new std::byte[allocation_size];
    auto* packed = new (raw) PackedPrimaryKeys(record_count, payload_bytes);
    return MutablePtr(packed, Deleter{});
  }

  static Ptr FromOne(std::string_view key) {
    const size_t payload_bytes = EncodedRecordSize(key);
    auto packed = AllocateMutable(
        1, CheckFitsUint32(payload_bytes,
                           "packed primary-key list bytes"));
    [[maybe_unused]] char* out = WriteRecord(packed->MutableRecords(), key);
    assert(out == packed->MutableRecords() + packed->bytes);
    return packed;
  }

  static uint32_t CheckFitsUint32(size_t value, const char* field) {
    if (value > std::numeric_limits<uint32_t>::max()) {
      throw std::length_error(field);
    }
    return static_cast<uint32_t>(value);
  }

  static size_t VarintSize(size_t value) {
    size_t size = 1;
    while (value >= 0x80) {
      value >>= 7;
      ++size;
    }
    return size;
  }

  static size_t EncodedRecordSize(std::string_view key) {
    return VarintSize(key.size()) + key.size();
  }

  static char* WriteVarint(char* out, size_t value) {
    while (value >= 0x80) {
      *out++ = static_cast<char>((value & 0x7f) | 0x80);
      value >>= 7;
    }
    *out++ = static_cast<char>(value);
    return out;
  }

  static char* WriteRecord(char* out, std::string_view key) {
    out = WriteVarint(out, key.size());
    if (!key.empty()) {
      std::memcpy(out, key.data(), key.size());
      out += key.size();
    }
    return out;
  }

  static Record DecodeRecord(const char* start, const char* limit) {
    const char* cursor = start;
    size_t length = 0;
    unsigned shift = 0;
    while (cursor != limit) {
      const unsigned char byte = static_cast<unsigned char>(*cursor++);
      length |= static_cast<size_t>(byte & 0x7f) << shift;
      if ((byte & 0x80) == 0) {
        assert(static_cast<size_t>(limit - cursor) >= length);
        return Record{start, cursor, cursor + length, length};
      }
      shift += 7;
      assert(shift < sizeof(size_t) * 8);
    }
    assert(false && "truncated packed primary-key list varint");
    return Record{start, limit, limit, 0};
  }

  char* MutableRecords() {
    return reinterpret_cast<char*>(this) + sizeof(PackedPrimaryKeys);
  }
};

static_assert(sizeof(PackedPrimaryKeys) == sizeof(uint32_t) * 2,
              "PackedPrimaryKeys must remain a two-field header");
static_assert(std::is_standard_layout<PackedPrimaryKeys>::value,
              "PackedPrimaryKeys must remain a standard-layout header");

/** @brief Zero-copy view over a `PackedPrimaryKeys` primary-key list. */
class PackedPrimaryKeysView {
 public:
  /** @brief Forward iterator yielding `std::string_view` values into the list. */
  class iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::string_view;
    using difference_type = std::ptrdiff_t;
    using reference = std::string_view;
    using pointer = void;

    iterator() = default;

    /**
     * @brief Returns the current primary key.
     * @return `std::string_view` into the viewed primary-key list.
     */
    reference operator*() const {
      const auto record = PackedPrimaryKeys::DecodeRecord(cursor_, limit_);
      return std::string_view(record.value, record.length);
    }

    /**
     * @brief Advances to the next primary key.
     * @return Reference to this iterator.
     */
    iterator& operator++() {
      assert(remaining_ != 0);
      const auto record = PackedPrimaryKeys::DecodeRecord(cursor_, limit_);
      cursor_ = record.next;
      --remaining_;
      return *this;
    }

    /**
     * @brief Advances to the next primary key.
     * @return Iterator value before the increment.
     */
    iterator operator++(int) {
      iterator previous = *this;
      ++*this;
      return previous;
    }

    /**
     * @brief Compares iterator position and remaining record count.
     * @param rhs Iterator to compare with.
     * @return True when both iterators refer to the same position.
     */
    bool operator==(const iterator& rhs) const {
      return cursor_ == rhs.cursor_ && remaining_ == rhs.remaining_;
    }

    /**
     * @brief Compares iterator position and remaining record count.
     * @param rhs Iterator to compare with.
     * @return True when the iterators differ.
     */
    bool operator!=(const iterator& rhs) const { return !(*this == rhs); }

   private:
    friend class PackedPrimaryKeysView;

    iterator(const char* cursor, const char* limit, uint32_t remaining)
        : cursor_(cursor), limit_(limit), remaining_(remaining) {}

    const char* cursor_ = nullptr;
    const char* limit_ = nullptr;
    uint32_t remaining_ = 0;
  };

  /**
   * @brief Constructs a view over a raw primary-key list pointer.
   * @param keys Primary-key list to view, or null for an empty view.
   */
  explicit PackedPrimaryKeysView(const PackedPrimaryKeys* keys = nullptr)
      : keys_(keys) {}

  /**
   * @brief Constructs a view over a shared primary-key list.
   * @param keys Primary-key list to view, or null for an empty view.
   */
  explicit PackedPrimaryKeysView(const PackedPrimaryKeys::Ptr& keys)
      : keys_(keys.get()) {}

  /**
   * @brief Checks whether the view contains no keys.
   * @return True when `size() == 0`.
   *
   * Runs in O(1).
   */
  bool empty() const { return size() == 0; }

  /**
   * @brief Returns the number of keys in the view.
   * @return Key count.
   *
   * Runs in O(1).
   */
  size_t size() const { return keys_ ? keys_->count : 0; }

  /**
   * @brief Returns an iterator to the first key.
   * @return Iterator whose dereference yields `std::string_view` into the list.
   */
  iterator begin() const {
    if (!keys_) return iterator();
    const char* const start = keys_->Records();
    return iterator(start, start + keys_->bytes, keys_->count);
  }

  /**
   * @brief Returns the past-the-end iterator.
   * @return Iterator marking the end of the viewed primary-key list.
   */
  iterator end() const {
    if (!keys_) return iterator();
    const char* const limit = keys_->Records() + keys_->bytes;
    return iterator(limit, limit, 0);
  }

  /**
   * @brief Finds the first key not less than `key`.
   * @param key Primary key to search for.
   * @return Iterator to the first matching or greater key, or `end()`.
   */
  iterator lower_bound(std::string_view key) const {
    for (auto it = begin(); it != end(); ++it) {
      if (!(*it < key)) return it;
    }
    return end();
  }

  /**
   * @brief Checks whether `key` is present.
   * @param key Primary key to search for.
   * @return True when the view contains `key`.
   */
  bool contains(std::string_view key) const {
    const auto it = lower_bound(key);
    return it != end() && *it == key;
  }

  /**
   * @brief Compares this view with another key-by-key.
   * @param rhs View to compare with.
   * @return True when both views contain the same keys in the same order.
   */
  bool equals(PackedPrimaryKeysView rhs) const {
    if (size() != rhs.size()) return false;
    auto lhs_it = begin();
    auto rhs_it = rhs.begin();
    for (; lhs_it != end(); ++lhs_it, ++rhs_it) {
      if (*lhs_it != *rhs_it) return false;
    }
    return true;
  }

 private:
  const PackedPrimaryKeys* keys_;
};

}  // namespace LineairDB

#endif  // LINEAIRDB_PACKED_PRIMARY_KEYS_HPP
