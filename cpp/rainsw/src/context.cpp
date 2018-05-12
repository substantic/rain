#include "context.h"
#include <sstream>

rainsw::Context::Context(size_t n_args) : n_args(n_args), error(false)
{

}

bool rainsw::Context::check_n_args(size_t n)
{
    if (n == n_args) {
        return true;
    }
    std::stringstream s;
    s << "Invalid number of arguments, expected = " << n << ", but got = " << n_args;
    set_error(s.str());
    return false;
}

void rainsw::Context::set_error(const std::string &message)
{
    error = true;
    error_message = message;
}
