#ifndef RAINSW_LOG_H
#define RAINSW_LOG_H

#include "spdlog/spdlog.h"

namespace rainsw {
    extern std::shared_ptr<spdlog::logger> logger;
    void log_errno_and_exit(const char *tmp)  __attribute__ ((noreturn));
}

#endif // LOG_H
