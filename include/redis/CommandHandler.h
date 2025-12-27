#ifndef REDIS_COMMAND_HANDLER_H
#define REDIS_COMMAND_HANDLER_H

#include <memory>
#include <string>
#include <vector>

namespace redis {

class Config;
class Storage;

class CommandHandler {
public:
  CommandHandler(const std::shared_ptr<Config> &config,
                 const std::shared_ptr<Storage> &storage);

  std::string handleCommand(const std::vector<std::string> &command) const;

private:
  std::shared_ptr<Config> config_;
  std::shared_ptr<Storage> storage_;

  static std::string handlePing();
  static std::string handleEcho(const std::vector<std::string> &args);
  std::string handleSet(const std::vector<std::string> &args) const;
  std::string handleGet(const std::vector<std::string> &args) const;
  std::string handleConfig(const std::vector<std::string> &args) const;
  std::string handleKeys(const std::vector<std::string> &args) const;
  std::string handleInfo(const std::vector<std::string> &args) const;
  static std::string handleReplconf(const std::vector<std::string> &args);
  static std::string handlePsync(const std::vector<std::string> &args);
};

} // namespace redis

#endif // REDIS_COMMAND_HANDLER_H