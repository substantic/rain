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

tasklib::Connection::Connection()
{
    int s = ::socket(PF_UNIX, SOCK_STREAM, 0);

    if (!s) {
        log_errno_and_exit("Cannot create unix socket");
    }

    this->socket = s;
}

tasklib::Connection::~Connection() {
   close(socket);
}

void tasklib::Connection::connect(const char *socket_path)
{
   struct sockaddr_un server_addr;
   bzero(&server_addr, sizeof(server_addr));
   server_addr.sun_family = AF_UNIX;
   strncpy(server_addr.sun_path, socket_path, sizeof(server_addr.sun_path) - 1);

   if (::connect(socket, (const struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
       log_errno_and_exit("Cannot connect to unix socket");
   }
}

static void send_all(int socket, const unsigned char *data, size_t len) {
   while (len > 0)
   {
       int i = ::send(socket, data, len, 0);
       if (i < 1) {
           tasklib::log_errno_and_exit("Sending data failed");
       }
       data += i;
       len -= i;
   }
}

void tasklib::Connection::send(const unsigned char * data, size_t len)
{
   // TODO: Fix this on big-endian machines
   uint32_t size = len;
   send_all(socket, reinterpret_cast<const unsigned char*>(&size), sizeof(uint32_t));
   send_all(socket, data, len);
}

std::vector<char> tasklib::Connection::receive()
{
    const size_t READ_AT_ONCE = 128 * 1024;
    for(;;) {
        auto sz = buffer.size();
        if (sz >= sizeof(uint32_t)) {
            // TODO: fix this on big-endian machines
            uint32_t *len_ptr = reinterpret_cast<uint32_t*>(&buffer[0]);
            size_t len = *len_ptr + sizeof(uint32_t);
            if (sz >= len) {
                std::vector<char> result(buffer.begin() + sizeof(uint32_t), buffer.begin() + len);
                buffer.erase(buffer.begin(), buffer.begin() + len);
                return result;
            }
        }

        buffer.resize(sz + READ_AT_ONCE);
        int r = ::read(socket, &buffer[sz], READ_AT_ONCE);
        if (r < 0) {
            tasklib::log_errno_and_exit("Reading data failed");
        }
        if (r == 0) {
            logger->critical("Connection to server closed");
            exit(1);
        }
        buffer.resize(sz + r);
    }
}

