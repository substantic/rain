#include "subworker.h"
#include "log.h"

rainsw::Subworker::Subworker()
{
    spdlog::set_pattern("%H:%M:%S [%l] %v");
    spdlog::set_level(spdlog::level::debug);


    char *socket_path = std::getenv("RAIN_SOCKET");
    if (!socket_path) {
        logger->error("Env variable 'RAIN_SOCKET' not found");
        logger->error("It seems that subworker is not running in Rain environment");
        exit(1);
    }

    connection = std::make_unique<Connection>(socket_path);
}
