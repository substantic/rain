@0xb3195a92eff52478;

using import "common.capnp".TaskId;
using import "common.capnp".GovernorId;
using import "common.capnp".DataObjectId;
using import "common.capnp".SessionId;
using import "common.capnp".TaskState;
using import "common.capnp".DataObjectState;
using import "common.capnp".UnitResult;
using import "common.capnp".Resources;
using import "common.capnp".DataType;
using import "common.capnp".FetchResult;

struct GovernorInfo {
    governorId @0: GovernorId;
    tasks @1 :List(TaskId);
    objects @2 :List(DataObjectId);
    objectsToDelete @3 :List(DataObjectId);
    resources @4 :Resources;
}

struct ServerInfo {
  governors @0 :List(GovernorInfo);
}

interface ClientService {
    getServerInfo @0 () -> ServerInfo;
    # Get information about server

    newSession @1 () -> (sessionId: SessionId);
    # Ask for a new session

    closeSession @2 (sessionId :SessionId) -> ();
    # Remove session from governor, all running tasks are stopped,
    # all existing data objects are removed

    submit @3 (tasks :List(Task), objects :List(DataObject)) -> ();
    # Submit new tasks and data objects into server
    # allTaskId / allDataObjectsId is NOT allowed

    unkeep @4 (objectIds :List(DataObjectId)) -> UnitResult;
    # Removed "keep" flag from data objects
    # It is an error if called for non-keep object
    # allDataObjectsId is allowed

    wait @5 (taskIds :List(TaskId), objectIds: List(DataObjectId)) -> UnitResult;
    # Wait until all given data objects are not produced
    # and all given task finished.
    # allTaskId / allDataObjectsId is allowed

    waitSome @6 (taskIds: List(TaskId),
                 objectIds: List(DataObjectId)) -> (
                             finishedTasks: List(TaskId),
                             finishedObjects: List(DataObjectId));
    # Wait until at least one data object or task is not finished.
    # It may return more objects/tasks at once.
    # finished_tasks and finished_objects are both returned empty
    # only if taskIds and objectsIds are empty.
    # allTaskId / allDataObjectsId is allowed

    getState @7 (taskIds: List(TaskId),
               objectIds: List(DataObjectId)) -> Update;
    # Get current state of tasks and objects
    # allTaskId / allDataObjectsId is allowed

    terminateServer @8 () -> ();
    # Quit server; the connection to the server will be closed after this call

    fetch @9 (id :DataObjectId, includeInfo :Bool, offset :UInt64, size :UInt64) -> FetchResult;
}

struct Update {
    tasks @0 :List(TaskUpdate);
    objects @1 :List(DataObjectUpdate);
    state @2 :UnitResult;

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

struct Task {
    spec @0: Text;
}

struct DataObject {
    spec @0: Text;
    keep @1 :Bool;
    hasData @2: Bool;
    data @3 :Data;
}
