@0xbf6d1c73c7ecc3b4;

# NOTE: All structures are primarily intended for Server <-> Worker comm.
# Client may want any kind of richer operations that may make sense later
# Also, for now client does know the entire graph so not much sense sending it back
# (and if, then with a lot of additional info).

# Additonal data - stats, plugin data, user data, ...
# TODO: Specify in a better and extensible way.
#       Consider embedding CBOR, MSGPACK, ... as Data.

struct Additional {
    items @0 :List(Item);

    struct Item {
        nkey @0 :Text;
        value :union {
            int @1 :Int64;
            float @2 :Float64;
            text @3 :Text;
            data @4 :Data;
        }
    }
}

# Unique identifier pair: session ID + Task/Object ID
# Negative values are reserved and should not be normally used
# TaskId and ObjectId are distinguished on purpose to allow better type-checking in
# bindings

struct TaskId {
    id @0 :Int32;
    sessionId @1 :Int32;
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

# Task instance
# Sent: Client -> Server (submit or update)
#       Server -> Worker (submit or update, worker-local tasks only)
#       Server -> Client (graph query, future feature?)
# NOTE: From the above, only S->C communicates state with data, S<->W just send updates
# and initially the state is implicit (NA on Server, Assigned on Worker)
# NOTE: Consider adding State and/or Placement (but see above)

struct Task {
    id @0 :TaskId;
    inputs @1 :List(LabelledDataObject);
    outputs @2 :List(LabelledDataObject);
    procedureKey @3 :Text;
    procedureConfig @4 :Data;
    additional @5: Additional;

    # Labels for Sid-referenced inputs
    struct LabelledDataObject {
        id @0 :DataObjectId;
        label @1 :Text;
    }
}

# Data object instance information (not the data)
# Sent: Client <-> Server and Server -> Worker


struct DataObject {
    id @0 :DataObjectId;
    # Optional, if noTask, then this DataObject is a constant
    producer @1 :TaskId = .noTask;
    placement @2 :WorkerId;
    size @3 :Int64 = -1; # Positive if known
    additional @4: Additional;

    enum State {
        # TODO
    }
}


