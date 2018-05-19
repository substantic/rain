#ifndef CONTEXT_H
#define CONTEXT_H

#include <string>

namespace tasklib {

class Context
{
public:
    Context(size_t n_args);
    bool check_n_args(size_t n);
    void set_error(const std::string &message);


    const bool has_error() const {
       return error;
    }

    const std::string& get_error_message() const {
        return error_message;
    }

protected:
    size_t n_args;
    bool error;
    std::string error_message;
};

}

#endif // CONTEXT_H
