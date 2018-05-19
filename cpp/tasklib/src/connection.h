#ifndef RAINSW_CONNECTION_H
#define RAINSW_CONNECTION_H

#include <stdlib.h>
#include <vector>

namespace tasklib {

class Connection
{
public:
    Connection();
    ~Connection();

    void connect(const char *socket_path);

    Connection(const Connection& that) = delete;
    Connection& operator=(Connection const&) = delete;

    void send(const unsigned char *data, size_t len);
    std::vector<char> receive();

private:
    int socket;
    std::vector<char> buffer;
};

}

#endif // RAINSW_CONNECTION_H
