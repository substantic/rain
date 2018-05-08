
#include "log.h"

namespace rainsw {
std::shared_ptr<spdlog::logger> logger = spdlog::stdout_logger_mt("rainsw");
}

void rainsw::log_errno_and_exit(const char *tmp)
{
    logger->critical("{}: {}", tmp, strerror(errno));
    exit(1);
}

