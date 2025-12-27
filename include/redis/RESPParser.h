#ifndef REDIS_RESP_PARSER_H
#define REDIS_RESP_PARSER_H

#include <string>
#include <vector>

namespace redis {

class RESPParser {
 public:
  static std::vector<std::string> parseArray(const std::string& data);
  static std::string parseSimpleString(const std::string& data);
  static std::string encodeSimpleString(const std::string& str);
  static std::string encodeBulkString(const std::string& str);
  static std::string encodeArray(const std::vector<std::string>& items);
  static std::string encodeError(const std::string& error);
  static std::string encodeNull();
};

}  // namespace redis

#endif  // REDIS_RESP_PARSER_H