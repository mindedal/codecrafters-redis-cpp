#ifndef REDIS_STORAGE_H
#define REDIS_STORAGE_H

#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace redis {

struct ValueWithExpiry {
  std::string value;
  std::chrono::steady_clock::time_point expiryTime;
  bool hasExpiry;

  ValueWithExpiry() : hasExpiry(false) {}
  explicit ValueWithExpiry(const std::string& val) : value(val), hasExpiry(false) {}
  ValueWithExpiry(const std::string& val,
                  std::chrono::steady_clock::time_point expiry)
      : value(val), expiryTime(expiry), hasExpiry(true) {}
};

class Storage {
 public:
  Storage() = default;

  void set(const std::string& key, const std::string& value);
  void setWithExpiry(const std::string& key, const std::string& value,
                     int64_t expiryMs);
  std::optional<std::string> get(const std::string& key);
  std::vector<std::string> getAllKeys();

 private:
  std::unordered_map<std::string, ValueWithExpiry> data_;
  mutable std::mutex mutex_;

  void removeExpiredKey(const std::string& key);
};

}  // namespace redis

#endif  // REDIS_STORAGE_H