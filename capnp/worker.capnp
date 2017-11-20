@0xba8b704c7d1a0017;

# Worker <-> Server and Worker <-> Worker communication.

using import "datastore.capnp".DataStore;
using import "common.capnp".WorkerId;
using import "common.capnp".TaskId;
using import "common.capnp".DataObjectId;
using import "common.capnp".Additionals;
using import "common.capnp".TaskState;
using import "common.capnp".DataObjectState;
using import "common.capnp".DataObjectType;
using import "common.capnp".Resources;
using import "monitor.capnp".MonitoringFrames;

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
        additionals @2 :Additionals;
    }

    struct DataObjectUpdate {
        id @0 :DataObjectId;
        state @1 :DataObjectState;
        size @2 :UInt64;
        # Only valid when the state is `finished` and `removed`, otherwise should be 0.
    }
}

struct WorkerInfo {
    tasks @0 :List(TaskId);
    objects @1 :List(DataObjectId);
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

    unassignObjects @2 (objects :List(DataObjectId)) -> ();

    stopTasks @3 (tasks :List(TaskId)) -> ();

    getWorkerResources @4 () -> Resources;

    getMonitoringFrames @5 () -> MonitoringFrames;

    getInfo @6 () -> WorkerInfo;

    # TODO: actual status: CPU, resources, counters, ...

    # TODO: Control worker (shutdown, pause) etc ...
}

# Task instance

struct Task {
    id @0 :TaskId;

    inputs @1 :List(InDataObject);
    outputs @2 :List(DataObjectId);

    taskType @3 :Text;

    taskConfig @4 :Data;

    additionals @5: Additionals;

    resources @6: Resources;
    # Number of request CPUs; will be replaced by more sophisticated
    # resource requests

    struct InDataObject {
        id @0 :DataObjectId;
        label @1 :Text;
        path @2 :Text;
    }
}

# Data object instance information (not the data)

struct DataObject {
    id @0 :DataObjectId;

    placement @1 :WorkerId;
    # If equal to local worker id, then local, otherwise remote.

    type @2 :DataObjectType;

    size @3 :Int64 = -1;
    # In bytes, positive if known.

    state @4 :DataObjectState;
    # Current object state. All input objects (external or local) should be `finished` or
    # `running` (when streaming), output objects `assigned`.

    assigned @5 :Bool;

    label @6 :Text;

    additionals @7 :Additionals;
}