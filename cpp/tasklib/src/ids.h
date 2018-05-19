#ifndef RAINSW_IDS_H
#define RAINSW_IDS_H

#include <stdint.h>
#include <string>
#include <cbor.h>

namespace tasklib {

using SessionId = uint32_t;
using Id = uint32_t;

class Sid {
public:
   Sid() : session_id(0), id(0) {}
   Sid(SessionId session_id, Id id) : session_id(session_id), id(id) {}

   Id get_id() const {
      return id;
   }

   SessionId get_session_id() const {
      return session_id;
   }

   bool is_valid() const {
      return session_id != 0 || id != 0;
   }

   static Sid from(cbor_item_t *item);

   std::string to_string() const;


private:
   SessionId session_id;
   Id id;
};

using TaskId = Sid;
using DataObjectId = Sid;

}

#endif // IDS_H

