#ifndef RAINSW_SUBWORKER_H
#define RAINSW_SUBWORKER_H

#include <memory>
#include <string>
#include "connection.h"
#include "cbor.h"

namespace rainsw {

class Subworker {

public:
    Subworker(const std::string &type);
    void start();

private:

    void init();
    void send_message(const char *name, cbor_item_t *data);
    void process_message(std::vector<char> &data);
    void process_message_call(cbor_item_t *msg_data);

    Connection connection;
    std::string type;


};

}

#endif // RAINSW_SUBWORKER_H
