#ifndef REDIS_RDB_PARSER_H
#define REDIS_RDB_PARSER_H

#include <cstdint>
#include <fstream>
#include <string>

namespace redis {

class Storage;

class RDBParser {
public:
  RDBParser() = default;

  bool parseFile(const std::string &filepath, Storage &storage);

private:
  std::ifstream file_;

  bool readHeader();
  bool skipMetadata();
  bool readDatabase(Storage &storage);

  uint8_t readByte();
  uint32_t readUInt32LE();
  uint64_t readUInt64LE();

  uint64_t readLength();
  std::string readString();

  bool skipBytes(size_t count);
  bool isEOF();
};

} // namespace redis

#endif // REDIS_RDB_PARSER_H