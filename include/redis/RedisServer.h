#ifndef REDIS_SERVER_H
#define REDIS_SERVER_H

#include <memory>
#include <set>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace redis {

#ifdef _WIN32
using socket_t = SOCKET;
#define INVALID_SOCKET_VAL INVALID_SOCKET
#define SOCKET_ERROR_VAL SOCKET_ERROR
#else
using socket_t = int;
#define INVALID_SOCKET_VAL -1
#define SOCKET_ERROR_VAL -1
#endif

class Config;
class Storage;
class CommandHandler;
class RDBParser;

class RedisServer {
 public:
  explicit RedisServer(const std::shared_ptr<Config>& config);
  ~RedisServer();

  void run();

 private:
  bool loadRDBFile() const;
  std::shared_ptr<Config> config_;
  std::shared_ptr<Storage> storage_;
  std::shared_ptr<CommandHandler> commandHandler_;

  socket_t serverFd_;
  std::set<socket_t> clientFds_;
  socket_t masterFd_;

  bool createServerSocket();
  bool connectToMaster();
  void handleNewConnection();
  void handleClientData(socket_t clientFd);
  void closeClient(socket_t clientFd);
};

}  // namespace redis

#endif  // REDIS_SERVER_H