@0xd7b1fdae7f8daa87;

# Both task and data object id share the same numbering space and must use distinct ids.
# Session id is assigned by the server. Objects from different sessions may not interact.
# Session id <0 has a special meaning.
# Task id and data object id are the same struct but are distinct types for type checking.

using SessionId = Int32;

const allTasksId :Int32 = -2;
const allDataObjectsId :Int32 = -2;

struct TaskId {
    id @0 :Int32;
    sessionId @1 :SessionId;

    const none :TaskId = ( sessionId = -1, id = 0 );
}

struct DataObjectId {
    id @0 :Int32;
    sessionId @1 :SessionId;

    const none :DataObjectId = ( sessionId = -1, id = 0 );
}

struct SocketAddress {
    # IPv4/6 address of a socket.
    port @0 :UInt16;
    address :union {
        ipv4 @1: Data; # Network-endian address (4 bytes)
        ipv6 @2: Data; # Network-endian address (16 bytes)
    }
}

using WorkerId = SocketAddress;
# Worker id is the address of the RPC listening port.

enum TaskState {
        notAssigned @0;
        ready @1;
        assigned @2;
        running @3;
        finished @4;
        failed @5;
}

enum DataObjectState {
    unfinished @0;
    finished @1;
    removed @2;
}

struct Attributes {
    items @0 :List(Item);

    struct Item {
        key @0 :Text;
        value @1: Text;
    }
}

struct Resources {
    nCpus @0 :UInt32;
}

struct Error {
    message @0 :Text;
    debug @1: Text;
    task @2: TaskId;
}

struct UnitResult {
        union {
            ok @0 :Void;
            error @1 :Error;
        }
}

struct Event {
    timestamp @0 :Timestamp;
    event @1: Text;
}

struct Timestamp {
    seconds @0 :UInt64;
    subsecNanos @1 :UInt32;
}