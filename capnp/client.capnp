@0xb3195a92eff52478;

using import "common.capnp".TaskId;
using import "common.capnp".WorkerId;
using import "common.capnp".DataObjectId;
using import "common.capnp".Additional;
using import "common.capnp".SessionId;

struct Info {
  nWorkers @0 :Int32;
}

interface ClientService {
  getInfo @0 () -> Info;
  newSession @1 () -> ClientSession;
}

interface ClientSession {
    submit @0 (tasks :List(Task), objects :List(DataObject)) -> ();
    wait
    fetch
    getId @42 () -> SessionId;
}

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


struct DataObject {
    # Data object instance information (not the data)
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
