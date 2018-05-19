#ifndef RAINSW_EXECUTOR_H
#define RAINSW_EXECUTOR_H

#include <memory>
#include <string>
#include <functional>
#include <unordered_map>

#include "connection.h"
#include "datainstance.h"
#include "cbor.h"
#include "context.h"

namespace tasklib {

using TaskFunction = std::function<void(Context&, DataInstanceVec&, DataInstanceVec&)>;

class Executor {

public:
    Executor(const std::string &type_name);
    void start();
    void add_task(const std::string &name, const TaskFunction &fn);

private:

    void init();
    void send_message(const char *name, cbor_item_t *data);
    void process_message(std::vector<char> &data);
    void process_message_call(cbor_item_t *msg_data);

    void send_error(const std::string &error_msg, cbor_item_t *id_item);

    Connection connection;
    std::string type_name;

    std::unordered_map<std::string, TaskFunction> registered_tasks;

};

}

#endif // RAINSW_EXECUTOR_H
