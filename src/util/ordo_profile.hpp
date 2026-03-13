#pragma once

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unistd.h>

namespace OrdoProfile {

struct Stat {
  uint64_t count{0};
  uint64_t total{0};
  uint64_t max{0};

  void Add(uint64_t value) {
    count++;
    total += value;
    if (value > max) max = value;
  }
};

inline uint64_t NowNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

class Recorder {
 public:
  static Recorder& Instance() {
    static Recorder recorder;
    return recorder;
  }

  static bool Enabled() {
    static const bool enabled = [] {
      const char* value = std::getenv("ORDO_PROFILE");
      return value != nullptr && value[0] != '\0' && value[0] != '0';
    }();
    return enabled;
  }

  void SetProcessName(std::string_view process_name) {
    if (!Enabled()) return;
    EnsureRegistered();
    std::lock_guard<std::mutex> lock(mu_);
    if (process_name_.empty()) {
      process_name_ = std::string(process_name);
    }
  }

  void AddDuration(std::string_view key, uint64_t ns) {
    if (!Enabled()) return;
    EnsureRegistered();
    std::lock_guard<std::mutex> lock(mu_);
    timers_[std::string(key)].Add(ns);
    MaybeFlushLocked();
  }

  void AddSample(std::string_view key, uint64_t value) {
    if (!Enabled()) return;
    EnsureRegistered();
    std::lock_guard<std::mutex> lock(mu_);
    samples_[std::string(key)].Add(value);
    MaybeFlushLocked();
  }

  void Dump() {
    if (!Enabled()) return;

    std::lock_guard<std::mutex> lock(mu_);
    const char* dir_env = std::getenv("ORDO_PROFILE_DIR");
    std::ofstream out(ProfilePathLocked(), std::ios::trunc);
    if (!out.is_open()) return;

    out << "kind\tkey\tcount\ttotal\tavg\tmax\n";
    for (const auto& [key, stat] : timers_) {
      const uint64_t avg = stat.count == 0 ? 0 : stat.total / stat.count;
      out << "timer_ns\t" << key << '\t' << stat.count << '\t' << stat.total
          << '\t' << avg << '\t' << stat.max << '\n';
    }
    for (const auto& [key, stat] : samples_) {
      const uint64_t avg = stat.count == 0 ? 0 : stat.total / stat.count;
      out << "sample\t" << key << '\t' << stat.count << '\t' << stat.total
          << '\t' << avg << '\t' << stat.max << '\n';
    }
  }

 private:
  Recorder() = default;

  void EnsureRegistered() {
    if (registered_.exchange(true)) return;
    std::atexit([] { Recorder::Instance().Dump(); });
  }

  void MaybeFlushLocked() {
    const uint64_t now_ns = NowNs();
    if (now_ns - last_flush_ns_ < 1000000000ULL) return;
    last_flush_ns_ = now_ns;
    std::ofstream out(ProfilePathLocked(), std::ios::trunc);
    if (!out.is_open()) return;

    out << "kind\tkey\tcount\ttotal\tavg\tmax\n";
    for (const auto& [key, stat] : timers_) {
      const uint64_t avg = stat.count == 0 ? 0 : stat.total / stat.count;
      out << "timer_ns\t" << key << '\t' << stat.count << '\t' << stat.total
          << '\t' << avg << '\t' << stat.max << '\n';
    }
    for (const auto& [key, stat] : samples_) {
      const uint64_t avg = stat.count == 0 ? 0 : stat.total / stat.count;
      out << "sample\t" << key << '\t' << stat.count << '\t' << stat.total
          << '\t' << avg << '\t' << stat.max << '\n';
    }
  }

  std::filesystem::path ProfilePathLocked() const {
    const char* dir_env = std::getenv("ORDO_PROFILE_DIR");
    std::filesystem::path dir =
        dir_env != nullptr ? std::filesystem::path(dir_env)
                           : std::filesystem::path("/tmp/ordo_profile");
    std::filesystem::create_directories(dir);

    std::string process = process_name_.empty() ? "process" : process_name_;
    return dir / (process + "_" +
                  std::to_string(static_cast<unsigned long>(::getpid())) + ".tsv");
  }

  std::mutex mu_;
  std::unordered_map<std::string, Stat> timers_;
  std::unordered_map<std::string, Stat> samples_;
  std::string process_name_;
  std::atomic<bool> registered_{false};
  uint64_t last_flush_ns_{0};
};

class ScopedTimer {
 public:
  explicit ScopedTimer(std::string key)
      : key_(std::move(key)), start_ns_(NowNs()), active_(Recorder::Enabled()) {}

  ~ScopedTimer() {
    if (!active_) return;
    Recorder::Instance().AddDuration(key_, NowNs() - start_ns_);
  }

 private:
  std::string key_;
  uint64_t start_ns_;
  bool active_;
};

inline void SetProcessName(std::string_view process_name) {
  Recorder::Instance().SetProcessName(process_name);
}

inline void AddDuration(std::string_view key, uint64_t ns) {
  Recorder::Instance().AddDuration(key, ns);
}

inline void AddSample(std::string_view key, uint64_t value) {
  Recorder::Instance().AddSample(key, value);
}

inline bool Enabled() { return Recorder::Enabled(); }

}  // namespace OrdoProfile
