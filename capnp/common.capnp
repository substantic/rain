@0xd7b1fdae7f8daa87;

struct TaskId {
    id @0 :Int32;
    sessionId @1 :Int32;

    const none :TaskId = ( sessionId = -1, id = 0 );
}

const noTask :TaskId = ( sessionId = -1, id = 0 );

struct DataObjectId {
    id @0 :Int32;
    sessionId @1 :Int32;
}

const noDataObjecy :DataObjectId = ( sessionId = -1, id = 0 );

struct WorkerId {
    port @0 :UInt16;
    address :union {
        ipv4 @1: Data; # Network-order address (4 bytes)
        ipv6 @2: Data; # Network-order address (16 bytes)
    }
}


struct Additional {
    # Additonal data - stats, plugin data, user data, ...
    # TODO: Specify in a better and extensible way.
    #       Consider embedding CBOR, MSGPACK, ... as Data.

    items @0 :List(Item);

    struct Item {
        key @0 :Text;
        value :union {
            int @1 :Int64;
            float @2 :Float64;
            text @3 :Text;
            data @4 :Data;
        }
    }
}
