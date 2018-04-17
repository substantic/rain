@0xf25243ae04134c6a;

using import "common.capnp".DataObjectId;
using import "common.capnp".TaskId;
using import "common.capnp".Attributes;
using import "common.capnp".DataType;

interface SubworkerControl {
    # This object serves also as bootstrap

    runTask @0 (task :Task) -> RunResponse;
    # Run the task, returns when task is finished
    # Flag "ok" indicates that task was finished without error
    # If "ok" if False then errorMessage is filled

    removeCachedObjects @1 (objectIds :List(DataObjectId)) -> ();
    # Remove object from Subworker
    # If object is "file" than the file is NOT removed, it is
    # a responsibility of the worker
}

interface SubworkerUpstream {

    register @0 (version :Int32,
                 subworkerId: Int32,
                 subworkerType: Text,
                 control :SubworkerControl) -> ();
    # Subworker ID is annoucted through environment variable RAIN_SUBWORKER_ID
    # We cannot assign subworker_id through RPC since ID has to be
    # allocated before process start, because we need to create files for redirection of stdout/stderr
    # and they already contains subworker_id in the name
}

struct Task {
    id @0 :TaskId;

    inputs @1 :List(InDataObject);
    outputs @2 :List(OutDataObject);

    attributes @3 :Attributes;

    struct InDataObject {
        id @0 :DataObjectId;
        data @1 :LocalData;
        label @2 :Text;
        saveInCache @3 :Bool;
    }

    struct OutDataObject {
        id @0 :DataObjectId;
        label @1 :Text;
        attributes @2 :Attributes;
    }
}

struct LocalData {

    attributes @0 :Attributes;

    storage :union {
        cache @1 :Void;
        # Data is cached in subworker

        memory @2 :Data;
        # Actual content of the data object

        path @3 :Text;
        # The object is on fs, the argument is path to object

        stream @4 :Void;
        # TODO

        inWorker @5 :DataObjectId;
        # This is used when subworker returns object to worker
        # we have just returned one of inputs
    }

    dataType @6:  DataType;
}

struct RunResponse {
    data @0 :List(LocalData);
    ok @1 :Bool;
    errorMessage @2 :Text;
    taskAttributes @3 :Attributes;
}