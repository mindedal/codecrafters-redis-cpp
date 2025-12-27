#include "redis/RedisServer.h"

#include <algorithm>
#include <cstring>
#include <iostream>

#include "redis/CommandHandler.h"
#include "redis/Config.h"
#include "redis/RDBParser.h"
#include "redis/RESPParser.h"
#include "redis/Storage.h"

#ifdef _WIN32
#define CLOSE_SOCKET(s) closesocket(s)
#else
#define CLOSE_SOCKET(s) close(s)
#endif

namespace redis {

RedisServer::RedisServer(const std::shared_ptr<Config> &config)
    : config_(config), storage_(std::make_shared<Storage>()),
      commandHandler_(std::make_shared<CommandHandler>(config, storage_)),
      serverFd_(INVALID_SOCKET_VAL), masterFd_(INVALID_SOCKET_VAL) {
#ifdef _WIN32
  WSADATA wsaData;
  if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
    std::cerr << "WSAStartup failed\n";
  }
#endif
}

RedisServer::~RedisServer() {
  for (const auto fd : clientFds_) {
    CLOSE_SOCKET(fd);
  }
  if (serverFd_ != INVALID_SOCKET_VAL) {
    CLOSE_SOCKET(serverFd_);
  }
  if (masterFd_ != INVALID_SOCKET_VAL) {
    CLOSE_SOCKET(masterFd_);
  }
#ifdef _WIN32
  WSACleanup();
#endif
}

bool RedisServer::createServerSocket() {
  serverFd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (serverFd_ == INVALID_SOCKET_VAL) {
    std::cerr << "Failed to create server socket\n";
    return false;
  }

  constexpr int reuse = 1;
  if (setsockopt(serverFd_, SOL_SOCKET, SO_REUSEADDR,
                 reinterpret_cast<const char *>(&reuse), sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return false;
  }

  struct sockaddr_in server_addr{};
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(config_->getPort());

  if (bind(serverFd_, reinterpret_cast<struct sockaddr *>(&server_addr),
           sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port " << config_->getPort() << std::endl;
    return false;
  }

  if (constexpr int connection_backlog = 5;
      listen(serverFd_, connection_backlog) != 0) {
    std::cerr << "listen failed" << std::endl;
    return false;
  }

  std::cout << "Server listening on port " << config_->getPort() << "..."
            << std::endl;
  return true;
}

bool RedisServer::connectToMaster() {
  masterFd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (masterFd_ == INVALID_SOCKET_VAL) {
    std::cerr << "Failed to create socket for master connection\n";
    return false;
  }

  sockaddr_in master_addr{};
  master_addr.sin_family = AF_INET;
  master_addr.sin_port = htons(config_->getMasterPort());

#ifdef _WIN32
  // Prefer InetPton + getaddrinfo over deprecated inet_addr/gethostbyname.
  {
    IN_ADDR addr{};
    if (InetPtonA(AF_INET, config_->getMasterHost().c_str(), &addr) == 1) {
      master_addr.sin_addr = addr;
    } else {
      addrinfo hints{};
      hints.ai_family = AF_INET;
      hints.ai_socktype = SOCK_STREAM;

      addrinfo *result = nullptr;
      if (getaddrinfo(config_->getMasterHost().c_str(), nullptr, &hints,
                      &result) != 0 ||
          result == nullptr) {
        std::cerr << "Failed to resolve master hostname: "
                  << config_->getMasterHost() << "\n";
        CLOSE_SOCKET(masterFd_);
        masterFd_ = INVALID_SOCKET_VAL;
        return false;
      }

      auto *ipv4 = reinterpret_cast<sockaddr_in *>(result->ai_addr);
      master_addr.sin_addr = ipv4->sin_addr;
      freeaddrinfo(result);
    }
  }
  // end _WIN32 resolver block
#else
  if (inet_pton(AF_INET, config_->getMasterHost().c_str(),
                &master_addr.sin_addr) <= 0) {
    const struct hostent *host =
        gethostbyname(config_->getMasterHost().c_str());
    if (host == nullptr) {
      std::cerr << "Failed to resolve master hostname: "
                << config_->getMasterHost() << "\n";
      CLOSE_SOCKET(masterFd_);
      masterFd_ = INVALID_SOCKET_VAL;
      return false;
    }
    memcpy(&master_addr.sin_addr, host->h_addr, host->h_length);
  }
#endif

  if (connect(masterFd_, reinterpret_cast<struct sockaddr *>(&master_addr),
              sizeof(master_addr)) < 0) {
    std::cerr << "Failed to connect to master at " << config_->getMasterHost()
              << ":" << config_->getMasterPort() << "\n";
    CLOSE_SOCKET(masterFd_);
    masterFd_ = INVALID_SOCKET_VAL;
    return false;
  }

  std::cout << "Connected to master at " << config_->getMasterHost() << ":"
            << config_->getMasterPort() << "\n";

  const std::string pingCommand = RESPParser::encodeArray({"PING"});
  if (send(masterFd_, pingCommand.c_str(), static_cast<int>(pingCommand.size()),
           0) < 0) {
    std::cerr << "Failed to send PING to master\n";
    CLOSE_SOCKET(masterFd_);
    masterFd_ = INVALID_SOCKET_VAL;
    return false;
  }

  char buffer[256];
  int bytesRead = recv(masterFd_, buffer, sizeof(buffer), 0);
  if (bytesRead <= 0) {
    std::cerr << "Failed to receive PONG from master\n";
    CLOSE_SOCKET(masterFd_);
    masterFd_ = INVALID_SOCKET_VAL;
    return false;
  }

  const std::string pongResponse(buffer, bytesRead);
  std::string parsedResponse = RESPParser::parseSimpleString(pongResponse);
  if (parsedResponse != "PONG") {
    std::cerr << "Unexpected response to PING: " << parsedResponse << "\n";
    CLOSE_SOCKET(masterFd_);
    masterFd_ = INVALID_SOCKET_VAL;
    return false;
  }

  std::cout << "Received PONG from master\n";

  const std::string replconfPort = RESPParser::encodeArray(
      {"REPLCONF", "listening-port", std::to_string(config_->getPort())});
  if (send(masterFd_, replconfPort.c_str(),
           static_cast<int>(replconfPort.size()), 0) < 0) {
    std::cerr << "Failed to send REPLCONF listening-port to master\n";
    CLOSE_SOCKET(masterFd_);
    masterFd_ = INVALID_SOCKET_VAL;
    return false;
  }

  bytesRead = recv(masterFd_, buffer, sizeof(buffer), 0);
  if (bytesRead <= 0) {
    std::cerr << "Failed to receive response to REPLCONF listening-port\n";
    CLOSE_SOCKET(masterFd_);
    masterFd_ = INVALID_SOCKET_VAL;
    return false;
  }

  const std::string okResponse1(buffer, bytesRead);
  parsedResponse = RESPParser::parseSimpleString(okResponse1);
  if (parsedResponse != "OK") {
    std::cerr << "Unexpected response to REPLCONF listening-port: "
              << parsedResponse << "\n";
    CLOSE_SOCKET(masterFd_);
    masterFd_ = INVALID_SOCKET_VAL;
    return false;
  }

  std::cout << "Sent REPLCONF listening-port\n";

  const std::string replconfCapa =
      RESPParser::encodeArray({"REPLCONF", "capa", "psync2"});
  if (send(masterFd_, replconfCapa.c_str(),
           static_cast<int>(replconfCapa.size()), 0) < 0) {
    std::cerr << "Failed to send REPLCONF capa to master\n";
    CLOSE_SOCKET(masterFd_);
    masterFd_ = INVALID_SOCKET_VAL;
    return false;
  }

  bytesRead = recv(masterFd_, buffer, sizeof(buffer), 0);
  if (bytesRead <= 0) {
    std::cerr << "Failed to receive response to REPLCONF capa\n";
    CLOSE_SOCKET(masterFd_);
    masterFd_ = INVALID_SOCKET_VAL;
    return false;
  }

  const std::string okResponse2(buffer, bytesRead);
  parsedResponse = RESPParser::parseSimpleString(okResponse2);
  if (parsedResponse != "OK") {
    std::cerr << "Unexpected response to REPLCONF capa: " << parsedResponse
              << "\n";
    CLOSE_SOCKET(masterFd_);
    masterFd_ = INVALID_SOCKET_VAL;
    return false;
  }

  std::cout << "Sent REPLCONF capa psync2\n";

  const std::string psyncCommand =
      RESPParser::encodeArray({"PSYNC", "?", "-1"});
  if (send(masterFd_, psyncCommand.c_str(),
           static_cast<int>(psyncCommand.size()), 0) < 0) {
    std::cerr << "Failed to send PSYNC to master\n";
    CLOSE_SOCKET(masterFd_);
    masterFd_ = INVALID_SOCKET_VAL;
    return false;
  }

  bytesRead = recv(masterFd_, buffer, sizeof(buffer), 0);
  if (bytesRead <= 0) {
    std::cerr << "Failed to receive response to PSYNC\n";
    CLOSE_SOCKET(masterFd_);
    masterFd_ = INVALID_SOCKET_VAL;
    return false;
  }

  std::string fullresyncResponse(buffer, bytesRead);
  (void)fullresyncResponse;

  std::cout << "Sent PSYNC ? -1\n";
  std::cout << "Handshake with master completed successfully\n";
  return true;
}

void RedisServer::run() {
  if (!createServerSocket()) {
    return;
  }

  // If we're a replica, connect to master
  if (config_->isReplica()) {
    if (!connectToMaster()) {
      std::cerr << "Failed to connect to master, exiting\n";
      return;
    }
  }

  std::cout << "Logs from your program will appear here!" << std::endl;

  while (true) {
    fd_set readFds;
    FD_ZERO(&readFds);
    FD_SET(serverFd_, &readFds);

#ifdef _WIN32
    socket_t maxFd = serverFd_;
    for (const socket_t clientFd : clientFds_) {
      FD_SET(clientFd, &readFds);
      maxFd = (std::max)(maxFd, clientFd);
    }

    const int activity = select(0, &readFds, nullptr, nullptr, nullptr);
#else
    int maxFd = serverFd_;
    for (const int clientFd : clientFds_) {
      FD_SET(clientFd, &readFds);
      maxFd = std::max(maxFd, clientFd);
    }

    const int activity = select(maxFd + 1, &readFds, nullptr, nullptr, nullptr);
#endif

    if (activity < 0) {
      std::cerr << "select error" << std::endl;
      break;
    }

    if (FD_ISSET(serverFd_, &readFds)) {
      handleNewConnection();
    }

    for (auto clientFdsSnapshot = clientFds_;
         const socket_t clientFd : clientFdsSnapshot) {
      if (FD_ISSET(clientFd, &readFds)) {
        handleClientData(clientFd);
      }
    }
  }
}

void RedisServer::handleNewConnection() {
  struct sockaddr_in client_addr{};
#ifdef _WIN32
  int client_addr_len = sizeof(client_addr);
#else
  socklen_t client_addr_len = sizeof(client_addr);
#endif
  const socket_t clientFd =
      accept(serverFd_, reinterpret_cast<struct sockaddr *>(&client_addr),
             &client_addr_len);

  if (clientFd == INVALID_SOCKET_VAL) {
    std::cerr << "Failed to accept client connection" << std::endl;
    return;
  }

  clientFds_.insert(clientFd);
  std::cout << "New client connected (fd: " << clientFd << ")" << std::endl;
}

void RedisServer::handleClientData(const socket_t clientFd) {
  char buffer[1024];
  const int bytesRead = recv(clientFd, buffer, sizeof(buffer), 0);

  if (bytesRead <= 0) {
    closeClient(clientFd);
    return;
  }

  const std::string data(buffer, bytesRead);

  if (const auto command = RESPParser::parseArray(data); !command.empty()) {
    const std::string response = commandHandler_->handleCommand(command);
    send(clientFd, response.c_str(), static_cast<int>(response.size()), 0);
  }
}

void RedisServer::closeClient(const socket_t clientFd) {
  CLOSE_SOCKET(clientFd);
  clientFds_.erase(clientFd);
  std::cout << "Client disconnected (fd: " << clientFd << ")" << std::endl;
}

bool RedisServer::loadRDBFile() const {
  const std::string rdbPath =
      config_->getDir() + "/" + config_->getDbFilename();

  if (RDBParser parser; !parser.parseFile(rdbPath, *storage_)) {
    std::cerr << "Failed to parse RDB file: " << rdbPath << std::endl;
    return false;
  }

  return true;
}

} // namespace redis