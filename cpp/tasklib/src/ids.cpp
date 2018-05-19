
#include "ids.h"
#include "log.h"

#include <sstream>

tasklib::Sid tasklib::Sid::from(cbor_item_t *item)
{
   int size = cbor_array_size(item);
   if (size != 2) {
      logger->critical("Sid should be array of size 2");
      exit(1);
   }
   cbor_item_t *i = cbor_array_get(item, 0);
   SessionId session_id = cbor_get_int(i);
   cbor_decref(&i);

   i = cbor_array_get(item, 1);
   Id id = cbor_get_int(i);
   cbor_decref(&i);

   return Sid(session_id, id);
}

std::string tasklib::Sid::to_string() const
{
   std::stringstream s;
   s << "[" << session_id << "," << id << "]";
   return s.str();
}
