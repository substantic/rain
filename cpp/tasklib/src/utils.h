#ifndef RAINSW_UTILS_H
#define RAINSW_UTILS_H

#include <cbor.h>
#include <string>

namespace tasklib {

   cbor_item_t* cb_map_lookup(const cbor_item_t *item, const char *name);
   std::string cb_map_lookup_string(const cbor_item_t *item, const char *name);
   std::string cb_to_string(const cbor_item_t *item);

   size_t file_size(const char *path);
}

#endif // UTILS_H

