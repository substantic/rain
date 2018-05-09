#ifndef RAINSW_IDS_H
#define RAINSW_IDS_H

#include <stdint.h>
#include "cbor.h"

using SessionId = uint32_t;
using Id = uint32_t;

class Sid {
public:
   Sid(SessionId session_id, Id id) : session_id(session_id), id(id) {}

   Id get_id() const {
      return id;
   }

   SessionId get_session_id() const {
      return session_id;
   }

   static Sid parse(cbor_item_t *item);

private:
   SessionId session_id;
   Id id;
};

#endif // IDS_H

