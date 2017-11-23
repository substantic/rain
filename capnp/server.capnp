@0xb01bcb96f4bd00be;

using import "client.capnp".ClientService;
using import "worker.capnp".WorkerControl;
using import "worker.capnp".WorkerUpstream;
using import "common.capnp".SocketAddress;
using import "common.capnp".WorkerId;
using import "common.capnp".Resources;

interface ServerBootstrap {
    registerAsClient @0 (version :Int32) -> (service :ClientService);
    # Registers as a client, verifies the API version and returns the Client interface.

    registerAsWorker @1 (version :Int32,
                         address :SocketAddress,
                         control: WorkerControl,
                         resources: Resources)
     -> (upstream :WorkerUpstream, workerId :WorkerId);
    # Registers as a worker, verifies the API version and returns the Worker upstream
    # interface (for calling the server with updates) and assigned worker id.
    # The `address` is the socket address with listening WorkerBootstrap interface.
    # If `address` is 0.0.0.0 or "::" (IPv6) (binding to all interfaces by
    # default), the server uses the peer address of the open connection.
}
