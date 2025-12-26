#include "redis/RDBParser.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>

#include "redis/Storage.h"

namespace redis {

bool RDBParser::parseFile(const std::string& filepath, Storage& storage) {
  if (!std::filesystem::exists(filepath)) {
    std::cout << "RDB file not found: " << filepath << std::endl;
    return true;  // Not an error - database starts empty
  }

  file_.open(filepath, std::ios::binary);
  if (!file_.is_open()) {
    std::cerr << "Failed to open RDB file: " << filepath << std::endl;
    return false;
  }

  bool success = false;

  if (readHeader() && skipMetadata() && readDatabase(storage)) {
    success = true;
  }

  file_.close();
  return success;
}

bool RDBParser::readHeader() {
  char header[9];
  file_.read(header, 9);

  if (!file_ || std::string(header, 5) != "REDIS") {
    std::cerr << "Invalid RDB file header" << std::endl;
    return false;
  }

  return true;
}

bool RDBParser::skipMetadata() {
  while (!isEOF()) {
    uint8_t type = readByte();

    if (type == 0xFA) {
      // Metadata subsection
      readString();  // metadata name
      readString();  // metadata value
    } else if (type == 0xFE || type == 0xFF) {
      // Database subsection starts or End of a file
      file_.seekg(-1, std::ios::cur);  // Go back one byte
      return true;
    } else {
      // Unknown type in the metadata section
      std::cerr << "Unexpected byte in metadata section: " << static_cast<int>(type)
                << std::endl;
      return false;
    }
  }

  return true;
}

bool RDBParser::readDatabase(Storage& storage) {
  while (!isEOF()) {
    if (const uint8_t type = readByte(); type == 0xFE) {
      // Check for hash table size info
      if (const uint8_t nextByte = readByte(); nextByte == 0xFB) {
        readLength();  // hash table size
        readLength();  // expire hash table size
      } else {
        file_.seekg(-1, std::ios::cur);
      }

      // Read key-value pairs
      while (!isEOF()) {
        uint8_t marker = readByte();

        if (marker == 0xFE || marker == 0xFF) {
          file_.seekg(-1, std::ios::cur);
          break;
        }

        // Check for expiry
        bool hasExpiry = false;
        uint64_t expiryTime = 0;

        if (marker == 0xFD) {
          // Expire in seconds
          expiryTime = readUInt32LE() * 1000;  // Convert to milliseconds
          hasExpiry = true;
          marker = readByte();  // Read value type
        } else if (marker == 0xFC) {
          // Expire in milliseconds
          expiryTime = readUInt64LE();
          hasExpiry = true;
          marker = readByte();  // Read value type
        }

        // For now, we only support string values (type 0)
        if (marker != 0x00) {
          std::cerr << "Unsupported value type: " << static_cast<int>(marker) << std::endl;
          return false;
        }

        std::string key = readString();
        std::string value = readString();

        if (hasExpiry) {
          // Convert Unix timestamp to duration from now
          auto now = std::chrono::system_clock::now();
          const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                           now.time_since_epoch())
                           .count();

          if (expiryTime > static_cast<uint64_t>(nowMs)) {
            const int64_t durationMs = expiryTime - nowMs;
            storage.setWithExpiry(key, value, durationMs);
          }
          // If expired, don't add to storage
        } else {
          storage.set(key, value);
        }
      }
    } else if (type == 0xFF) {
      // End of file marker
      skipBytes(8);  // Skip checksum
      return true;
    } else {
      std::cerr << "Unexpected byte in database section: " << static_cast<int>(type)
                << std::endl;
      return false;
    }
  }

  return true;
}

uint8_t RDBParser::readByte() {
  uint8_t byte;
  file_.read(reinterpret_cast<char*>(&byte), 1);
  return byte;
}

uint32_t RDBParser::readUInt32LE() {
  uint32_t value;
  file_.read(reinterpret_cast<char*>(&value), 4);
  return value;  // Assuming little-endian system
}

uint64_t RDBParser::readUInt64LE() {
  uint64_t value;
  file_.read(reinterpret_cast<char*>(&value), 8);
  return value;  // Assuming little-endian system
}

uint64_t RDBParser::readLength() {
  uint8_t firstByte = readByte();
  uint8_t type = (firstByte & 0xC0) >> 6;

  if (type == 0) {
    // Size is in the remaining 6 bits
    return firstByte & 0x3F;
  } else if (type == 1) {
    // Size is in the next 14 bits
    uint8_t secondByte = readByte();
    return ((firstByte & 0x3F) << 8) | secondByte;
  } else if (type == 2) {
    // Size is in the next 4 bytes (big-endian)
    uint32_t size = 0;
    for (int i = 0; i < 4; i++) {
      size = (size << 8) | readByte();
    }
    return size;
  } else {
    // Special encoding (type == 3)
    uint8_t format = firstByte & 0x3F;
    if (format == 0) {
      // 8-bit integer
      return readByte();
    } else if (format == 1) {
      // 16-bit integer
      uint16_t value;
      file_.read(reinterpret_cast<char*>(&value), 2);
      return value;
    } else if (format == 2) {
      // 32-bit integer
      uint32_t value;
      file_.read(reinterpret_cast<char*>(&value), 4);
      return value;
    }
  }

  return 0;
}

std::string RDBParser::readString() {
  uint8_t firstByte = readByte();
  file_.seekg(-1, std::ios::cur);  // Go back to re-read with readLength

  if ((firstByte & 0xC0) == 0xC0) {
    // Special encoding
    readByte();  // Skip the encoding byte
    uint8_t format = firstByte & 0x3F;

    if (format == 0) {
      // 8-bit integer
      uint8_t value = readByte();
      return std::to_string(value);
    } else if (format == 1) {
      // 16-bit integer
      uint16_t value;
      file_.read(reinterpret_cast<char*>(&value), 2);
      return std::to_string(value);
    } else if (format == 2) {
      // 32-bit integer
      uint32_t value;
      file_.read(reinterpret_cast<char*>(&value), 4);
      return std::to_string(value);
    }
  }

  // Regular string encoding
  uint64_t length = readLength();
  std::string str(length, '\0');
  file_.read(&str[0], length);
  return str;
}

bool RDBParser::skipBytes(size_t count) {
  file_.seekg(count, std::ios::cur);
  return file_.good();
}

bool RDBParser::isEOF() { return file_.eof() || file_.peek() == EOF; }

}  // namespace redis