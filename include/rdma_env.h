#pragma once

#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>

namespace diskann {

inline std::string get_env_string(const char* name,
                                  const std::string& fallback = "") {
  const char* value = std::getenv(name);
  return (value != nullptr && value[0] != '\0') ? std::string(value)
                                                : fallback;
}

inline uint16_t get_env_u16(const char* name, uint16_t fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr || value[0] == '\0') {
    return fallback;
  }

  char*         end = nullptr;
  unsigned long parsed = std::strtoul(value, &end, 10);
  if (end == value || *end != '\0' ||
      parsed > std::numeric_limits<uint16_t>::max()) {
    return fallback;
  }

  return static_cast<uint16_t>(parsed);
}

}  // namespace diskann
