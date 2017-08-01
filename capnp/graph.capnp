@0xbf6d1c73c7ecc3b4;

# NOTE: All structures are primarily intended for Server <-> Worker comm.
# Client may want any kind of richer operations that may make sense later
# Also, for now client does know the entire graph so not much sense sending it back
# (and if, then with a lot of additional info).

using import "common.capnp".TaskId;
using import "common.capnp".WorkerId;
using import "common.capnp".DataObjectId;
using import "common.capnp".Additional;



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
    producer @1 :TaskId;
    placement @2 :WorkerId;
    size @3 :Int64 = -1; # Positive if known
    additional @4: Additional;

    enum State {
        # TODO
    }
}


