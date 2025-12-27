#include "redis/CommandHandler.h"

#include <algorithm>
#include <cctype>

#include "redis/Config.h"
#include "redis/RESPParser.h"
#include "redis/Storage.h"

namespace redis {

CommandHandler::CommandHandler(const std::shared_ptr<Config>& config,
                               const std::shared_ptr<Storage>& storage)
    : config_(config), storage_(storage) {}

std::string CommandHandler::handleCommand(
    const std::vector<std::string>& command) const {
  if (command.empty()) {
    return RESPParser::encodeError("ERR empty command");
  }

  std::string cmd = command[0];
  std::ranges::transform(cmd, cmd.begin(), ::toupper);

  if (cmd == "PING") {
    return handlePing();
  } else if (cmd == "ECHO") {
    return handleEcho(
        std::vector<std::string>(command.begin() + 1, command.end()));
  } else if (cmd == "SET") {
    return handleSet(
        std::vector<std::string>(command.begin() + 1, command.end()));
  } else if (cmd == "GET") {
    return handleGet(
        std::vector<std::string>(command.begin() + 1, command.end()));
  } else if (cmd == "CONFIG") {
    return handleConfig(
        std::vector<std::string>(command.begin() + 1, command.end()));
  } else if (cmd == "KEYS") {
    return handleKeys(
        std::vector<std::string>(command.begin() + 1, command.end()));
  } else if (cmd == "INFO") {
    return handleInfo(
        std::vector<std::string>(command.begin() + 1, command.end()));
  } else if (cmd == "REPLCONF") {
    return handleReplconf(
        std::vector<std::string>(command.begin() + 1, command.end()));
  } else if (cmd == "PSYNC") {
    return handlePsync(
        std::vector<std::string>(command.begin() + 1, command.end()));
  } else {
    return RESPParser::encodeError("ERR unknown command '" + command[0] + "'");
  }
}

std::string CommandHandler::handlePing() {
  return RESPParser::encodeSimpleString("PONG");
}

std::string CommandHandler::handleEcho(const std::vector<std::string>& args) {
  if (args.empty()) {
    return RESPParser::encodeError(
        "ERR wrong number of arguments for 'echo' command");
  }
  return RESPParser::encodeBulkString(args[0]);
}

std::string CommandHandler::handleSet(
    const std::vector<std::string>& args) const {
  if (args.size() < 2) {
    return RESPParser::encodeError(
        "ERR wrong number of arguments for 'set' command");
  }

  const std::string& key = args[0];
  const std::string& value = args[1];

  if (args.size() >= 4) {
    std::string option = args[2];
    std::ranges::transform(option, option.begin(), ::toupper);

    if (option == "PX") {
      try {
        const int expiryMs = std::stoi(args[3]);
        storage_->setWithExpiry(key, value, expiryMs);
        return RESPParser::encodeSimpleString("OK");
      } catch ([[maybe_unused]] const std::exception& e) {
        return RESPParser::encodeError(
            "ERR invalid expire time in 'set' command");
      }
    }
  }

  storage_->set(key, value);
  return RESPParser::encodeSimpleString("OK");
}

std::string CommandHandler::handleGet(
    const std::vector<std::string>& args) const {
  if (args.empty()) {
    return RESPParser::encodeError(
        "ERR wrong number of arguments for 'get' command");
  }

  if (const auto value = storage_->get(args[0]); value.has_value()) {
    return RESPParser::encodeBulkString(value.value());
  } else {
    return RESPParser::encodeNull();
  }
}

std::string CommandHandler::handleConfig(
    const std::vector<std::string>& args) const {
  if (args.size() < 2) {
    return RESPParser::encodeError(
        "ERR wrong number of arguments for 'config' command");
  }

  std::string subcmd = args[0];
  std::ranges::transform(subcmd, subcmd.begin(), ::toupper);

  if (subcmd == "GET") {
    std::string param = args[1];
    std::ranges::transform(param, param.begin(), ::tolower);

    std::string value;
    if (param == "dir") {
      value = config_->getDir();
    } else if (param == "dbfilename") {
      value = config_->getDbFilename();
    } else {
      return RESPParser::encodeArray({});
    }

    return RESPParser::encodeArray({param, value});
  } else {
    return RESPParser::encodeError("ERR Unknown CONFIG subcommand");
  }
}

std::string CommandHandler::handleKeys(
    const std::vector<std::string>& args) const {
  if (args.empty()) {
    return RESPParser::encodeError(
        "ERR wrong number of arguments for 'keys' command");
  }

  // For now, only support the "*" pattern
  if (args[0] != "*") {
    return RESPParser::encodeError("ERR pattern not supported");
  }

  const auto keys = storage_->getAllKeys();
  return RESPParser::encodeArray(keys);
}

std::string CommandHandler::handleInfo(
    const std::vector<std::string>& args) const {
  // Check if the command has arguments and if it's "replication"
  if (!args.empty()) {
    std::string section = args[0];
    std::ranges::transform(section, section.begin(), ::tolower);

    if (section == "replication") {
      // Return replication info as a bulk string
      const std::string role = config_->isReplica() ? "slave" : "master";
      std::string info = "role:" + role + "\r\n";

      // Add master_replid and master_repl_offset for master nodes
      if (!config_->isReplica()) {
        info += "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n";
        info += "master_repl_offset:0";
      }

      return RESPParser::encodeBulkString(info);
    }
  }

  // For now, only support the replication section
  return RESPParser::encodeError("ERR wrong section for 'info' command");
}

std::string CommandHandler::handleReplconf(
    const std::vector<std::string>& args) {
  // For this challenge, we ignore the arguments
  // and just respond with +OK\r\n
  return RESPParser::encodeSimpleString("OK");
}

std::string CommandHandler::handlePsync(const std::vector<std::string>& args) {
  // PSYNC expects 2 arguments: replication_id and offset
  if (args.size() != 2) {
    return RESPParser::encodeError(
        "ERR wrong number of arguments for 'psync' command");
  }

  // For full resynchronization, we respond with:
  // +FULLRESYNC <REPL_ID> 0\r\n
  const std::string replId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
  const std::string offset = "0";
  const std::string response = "FULLRESYNC " + replId + " " + offset;

  return RESPParser::encodeSimpleString(response);
}

}  // namespace redis