@0xba8b704c7d1a0017;

# Governor <-> Server and Governor <-> Governor communication.

using import "common.capnp".GovernorId;
using import "common.capnp".TaskId;
using import "common.capnp".DataType;
using import "common.capnp".DataObjectId;
using import "common.capnp".TaskState;
using import "common.capnp".DataObjectState;
using import "common.capnp".Resources;
using import "common.capnp".Event;
using import "common.capnp".FetchResult;
using import "monitor.capnp".MonitoringFrames;


interface GovernorBootstrap {
    # Interface for entities connecting directly to the governor.
    # Currently only governors would do this but in the future, other entities may do this.

    fetch @0 (id :DataObjectId, includeInfo :Bool, offset :UInt64, size :UInt64) -> FetchResult;
}

struct GovernorStateUpdate {
    tasks @0 :List(TaskUpdate);
    objects @1 :List(DataObjectUpdate);

    struct TaskUpdate {
        id @0 :TaskId;
        state @1 :TaskState;
        info @2 :Text;
    }

    struct DataObjectUpdate {
        id @0 :DataObjectId;
        state @1 :DataObjectState;
        info @2 :Text;
    }
}

struct GovernorInfo {
    tasks @0 :List(TaskId);
    objects @1 :List(DataObjectId);
    objectsToDelete @2 :List(DataObjectId);
}

interface GovernorUpstream {
    # Every governor has one connection to the server. This is the interface that server
    # provides for messages from the governor.

    fetch @0 (id :DataObjectId, includeInfo :Bool, offset :UInt64, size :UInt64) -> FetchResult;

    updateStates @1 (update: GovernorStateUpdate) -> ();
    # Notify server about object state changes. The sizes are reported for
    # data objects that moved from `running` state to `finished` or directly to `removed`.

    getClientSession @2 () -> (session: Void); # TODO: return a real session
    # Gets a (temporary) client session that allows the tasks at the governor to modify
    # the graph. This is intended for subgraph expansion etc.

    pushEvents @3 (events :List(Event)) -> ();
    # Pushes events to server.
}

interface GovernorControl {
    # The server holds this interface at every governor, using it for all governor control
    # except for data pulls.

    addNodes @0 (newTasks :List(Task), newObjects :List(DataObject)) -> ();

    unassignObjects @1 (objects :List(DataObjectId)) -> ();

    stopTasks @2 (tasks :List(TaskId)) -> ();

    getGovernorResources @3 () -> Resources;

    getInfo @4 () -> GovernorInfo;
}

# Task instance

struct Task {
    spec @0: Text;
}

# Data object instance information (not the data)

struct DataObject {
    spec @0: Text;

    placement @1 :GovernorId;
    # If equal to local governor id, then local, otherwise remote.

    state @2 :DataObjectState;
    # Current object state. All input objects (external or local) should be `finished` or
    # `running` (when streaming), output objects `assigned`.

    assigned @3 :Bool;
}