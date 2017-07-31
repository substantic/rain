@0xba8b704c7d1a0017;

# Worker <-> Server and Worker <-> Worker communication.

using import "datastore.capnp".DataStore;
using import "common.capnp".WorkerId;

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

interface WorkerUpstream {
    # Every worker has one connection to the server. This is the interface that server
    # provides for messages from the worker.

    getDataStore @0 () -> (store :DataStore);
    # Returns the data handle of the server. It does not make sense to take more
    # than one instance of this.

    # TODO: Update graph status etc ...
}

interface WorkerControl {
    # The server holds this interface at every worker, using it for all worker control
    # except for data pulls.

    getDataStore @0 () -> (store :DataStore);
    # Returns the data handle of the worker. It does not make sense to take more than
    # one instance of this per worker.

    getAddress @1 () -> (address: WorkerId);
    # Get worker open port with WorkerBootstrap listening for other workers.

    # TODO: Update graph, control worker etc ...
}