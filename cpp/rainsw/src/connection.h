#ifndef RAINSW_CONNECTION_H
#define RAINSW_CONNECTION_H

#include <stdlib.h>

namespace rainsw {

class Connection
{
public:
    Connection(const char *socket_path);
    ~Connection();

    Connection(const Connection& that) = delete;
    Connection& operator=(Connection const&) = delete;

    void send(const char *data, size_t len);

private:
    int socket;
};

}

#endif // RAINSW_CONNECTION_H
