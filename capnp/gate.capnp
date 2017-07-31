@0xb01bcb96f4bd00be;

using import "client.capnp".ClientService;
using import "worker.capnp".WorkerControl;
using import "worker.capnp".WorkerUpstream;

interface Gate {
    registerAsClient @0 (version :Int32) -> (service :ClientService);
    # Registers as a client, verifies the API version and returns the Client interface.

    registerAsWorker @1 (version :Int32, interface: WorkerControl) ->
        (upstream :WorkerUpstream);
    # Registers as a worker, verifies the API version and returns the Worker upstream
    # interface (for calling the server with updates). The registering worker also
    # provides control interface for the server.
}
