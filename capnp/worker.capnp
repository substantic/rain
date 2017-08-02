@0xba8b704c7d1a0017;

# Worker <-> Server and Worker <-> Worker communication.

using import "datastore.capnp".DataStore;
using import "common.capnp".WorkerId;
using import "common.capnp".TaskId;
using import "common.capnp".DataObjectId;
using import "common.capnp".Additional;

interface WorkerBootstrap {
    # Interface for entities connecting directly to the worker.
    # Currently only workers would do this but in the future, other entities may do this.

    getDataStore @0 () -> (store :DataStore);
    # Returns the data handle of the server. It does not make sense to take more
    # than one instance of this.

    getWorkerControl @1 () -> (control :WorkerControl);
    # Directly get control interface of the worker. Not normally used but may be
    # internally handy.
}

struct WorkerStateUpdate {
    tasks @0 :List(TaskUpdate);
    objects @1 :List(DataObjectUpdate);

    struct TaskUpdate {
        id @0 :TaskId;
        state @1 :TaskState;
    }

    struct DataObjectUpdate {
        id @0 :DataObjectId;
        state @1 :DataObjectState;
        size @2 :UInt64;
        # Only valid when the state is `finished` and `removed`, otherwise should be 0.
    }
}

interface WorkerUpstream {
    # Every worker has one connection to the server. This is the interface that server
    # provides for messages from the worker.

    getDataStore @0 () -> (store :DataStore);
    # Returns the data handle of the server. It does not make sense to take more
    # than one instance of this.

    updateStates @1 (update: WorkerStateUpdate) -> ();
    # Notify server about object state changes. The sizes are reported for
    # data objects that moved from `running` state to `finished` or directly to `removed`.

    getClientSession @2 () -> (session: Void); # TODO: return a real session
    # Gets a (temporary) client session that allows the tasks at the worker to modify
    # the graph. This is intended for subgraph expansion etc.
}

interface WorkerControl {
    # The server holds this interface at every worker, using it for all worker control
    # except for data pulls.

    getDataStore @0 () -> (store :DataStore);
    # Returns the data handle of the worker. It does not make sense to take more than
    # one instance of this per worker.

    addNodes @1 (newTasks :List(Task), newObjects :List(DataObject)) -> ();

    removeNodes @2 (removeTasks :List(TaskId), removeObjects :List(DataObjectId)) -> ();

    getWorkerState @3 () -> (status: Void);
    # TODO: actual status: CPU, resources, counters, ...

    # TODO: Control worker (shutdown, pause) etc ...
}

# Task instance

struct Task {
    id @0 :TaskId;

    inputs @1 :List(IODataObject);
    outputs @2 :List(IODataObject);

    procedureKey @3 :Text;
    procedureConfig @4 :Data;

    additional @5: Additional;

    # Labels for inputs and outputs
    struct IODataObject {
        id @0 :DataObjectId;
        label @1 :Text;
        # NOTE: May add other attributes of the input/output (streaming?)
    }
}

enum TaskState {
        notAssigned @0;
        assigned @1;
        ready @2;
        running @3;
        finished @4;
}

# Data object instance information (not the data)

struct DataObject {
    id @0 :DataObjectId;

    producer @1 :TaskId;
    # Optional, if Task.none, then this DataObject is a constant

    placement @2 :WorkerId;
    # If equal to local worker id, then local, otherwise remote.

    size @3 :Int64 = -1;
    # In bytes, positive if known.

    state @4 :DataObjectState;
    # Current object state. All input objects (external or local) should be `finished` or
    # `running` (when streaming), output objects `assigned`.

    additional @5: Additional;
}

enum DataObjectState {
    notAssigned @0;
    assigned @1;
    running @2;
    finished @3;
    removed @4;
}


