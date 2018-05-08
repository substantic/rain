#ifndef RAINSW_SUBWORKER_H
#define RAINSW_SUBWORKER_H

#include <memory>
#include "connection.h"

namespace rainsw {

class Subworker {

private:
    Subworker();
    std::unique_ptr<Connection> connection;
};

}

#endif // RAINSW_SUBWORKER_H
