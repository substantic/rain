
#include "log.h"

namespace tasklib {
   std::shared_ptr<spdlog::logger> logger = spdlog::stdout_logger_mt("tasklib");
}

void tasklib::log_errno_and_exit(const char *tmp)
{
    logger->critical("{}: {}", tmp, strerror(errno));
    exit(1);
}

void tasklib::log_cbor_error_and_exit()
{
   logger->critical("cbor allocation failed");
   abort();
}


