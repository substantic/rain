@0xf25243ae04134c6a;

using import "worker.capnp".Task;
using import "common.capnp".DataObjectId;

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

    registerControl @2 (version :Int32, control :SubworkerControl) -> ();
}

struct LocalDataObject {
    id @0 :DataObjectId;

    union {
        file @1 :UInt64;
        # The object is in file, the argument is the size in bytes

        memory @2 :Data;
        # Actual content of the data object
    }
}