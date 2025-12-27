#ifndef REDIS_CONFIG_H
#define REDIS_CONFIG_H

#include <string>

namespace redis {

class Config {
public:
  Config();

  void parseArgs(int argc, char **argv);

  const std::string &getDir() const { return dir_; }
  const std::string &getDbFilename() const { return dbfilename_; }
  int getPort() const { return port_; }

  bool isReplica() const { return !masterHost_.empty(); }
  const std::string &getMasterHost() const { return masterHost_; }
  int getMasterPort() const { return masterPort_; }

private:
  std::string dir_;
  std::string dbfilename_;
  int port_;
  std::string masterHost_;
  int masterPort_;
};

} // namespace redis

#endif // REDIS_CONFIG_H