
#include "log.h"

namespace rainsw {
   std::shared_ptr<spdlog::logger> logger = spdlog::stdout_logger_mt("rainsw");
}

void rainsw::log_errno_and_exit(const char *tmp)
{
    logger->critical("{}: {}", tmp, strerror(errno));
    exit(1);
}

void rainsw::log_cbor_error_and_exit()
{
   logger->critical("cbor allocation failed");
   abort();
}


