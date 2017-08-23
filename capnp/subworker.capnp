@0xf25243ae04134c6a;

using import "worker.capnp".Task;
using import "common.capnp".DataObjectId;
using import "common.capnp".DataObjectType;
using import "datastore.capnp".Stream;

interface SubworkerControl {
    # This object serves also as bootstrap

    runTask @0 (task :Task) -> (objects: List(LocalDataObject));
    # Run the task, returns when task is finished

    removeLocalObjects @1 (objectIds :List(DataObjectId)) -> ();
    # Remove object from Subworker
    # If object is "file" than the file is NOT removed, it is
    # a responsibility of the worker
}

interface SubworkerUpstream {

    getDataObjectPath @0 () -> (path: Text);
    # Path for storing local data objects
    # This information is needed when subworker creates new objects

    pullLocalObjects @1 (objectIds :List(DataObjectId)) -> (objects: List(LocalDataObject));
    # Get local objects from worker to subworker

    register @2 (version :Int32, subworkerId: Int32, control :SubworkerControl) -> ();
    # Subworker ID is annoucted through environment variable RAIN_SUBWORKER_ID
    # We cannot assign subworker_id through RPC since ID has to be
    # allocated before process start, because we need to create files for redirection of stdout/stderr
    # and they already contains subworker_id in the name
}


struct LocalDataObject {
    id @0 :DataObjectId;
    type @1 :DataObjectType;

    union {
        filesystem @2 :Text;
        # The object is in file, the argument is the size in bytes

        memory @3 :Data;
        # Actual content of the data object

        stream @4 :Stream;
    }
}