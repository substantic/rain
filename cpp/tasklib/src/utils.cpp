
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "utils.h"
#include "log.h"

// !! Does not increment reference of returned object
cbor_item_t* tasklib::cb_map_lookup(const cbor_item_t *item, const char *name)
{
   size_t name_len = strlen(name);
   size_t size = cbor_map_size(item);
   cbor_pair *pairs = cbor_map_handle(item);
   for (size_t i = 0; i < size; i++) {
      const cbor_item_t *key = pairs[i].key;
      size_t len = cbor_string_length(key);
      auto *str = cbor_string_handle(key);
      if (name_len == len && !memcmp(name, str, len)) {
         return pairs[i].value;
      }
   }
   logger->critical("Cannot found key: {}", name);
   exit(1);
}

std::string tasklib::cb_map_lookup_string(const cbor_item_t *item, const char *name)
{
   return cb_to_string(cb_map_lookup(item, name));
}

std::string tasklib::cb_to_string(const cbor_item_t *item)
{
   return std::string(reinterpret_cast<const char*>(cbor_string_handle(item)),
                      cbor_string_length(item));
}

size_t tasklib::file_size(const char *path)
{
    struct stat info;
    if (stat(path, &info))
    {
        log_errno_and_exit("stat");
    }
    return info.st_size;
}
