#ifndef RAINSW_LOG_H
#define RAINSW_LOG_H

#include "spdlog/spdlog.h"
#include "cbor.h"
#include "common.h"

namespace tasklib {
    extern std::shared_ptr<spdlog::logger> logger;
    void log_errno_and_exit(const char *tmp)  __attribute__ ((noreturn));
    void log_cbor_error_and_exit() __attribute__ ((noreturn));

    inline void cbor_check(const cbor_item_t *value) {
       if (unlikely(!value)) {
          log_cbor_error_and_exit();
       }
    }
}

#endif // LOG_H
