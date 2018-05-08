#include "connection.h"

#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "log.h"

rainsw::Connection::Connection(const char *socket_path)
{
    struct sockaddr_un server_addr;

    bzero(&server_addr, sizeof(server_addr));
    server_addr.sun_family = AF_UNIX;
    strncpy(server_addr.sun_path, socket_path, sizeof(server_addr.sun_path) - 1);

    int s = ::socket(PF_UNIX, SOCK_STREAM, 0);

    if (!s) {
        log_errno_and_exit("Cannot create unix socket");
    }

    if (connect(s, (const struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        log_errno_and_exit("Cannot connect to unix socket");
    }

    this->socket = s;

}

rainsw::Connection::~Connection() {
    close(socket);
}

void rainsw::Connection::send(const char *data, size_t len)
{
    while (len > 0)
    {
        int i = ::send(socket, data, len, 0);
        if (i < 1) {
            log_errno_and_exit("Sending data failed");
        }
        data += i;
        len -= i;
    }
}
