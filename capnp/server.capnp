@0xb01bcb96f4bd00be;

using import "client.capnp".ClientService;
using import "governor.capnp".GovernorControl;
using import "governor.capnp".GovernorUpstream;
using import "common.capnp".SocketAddress;
using import "common.capnp".GovernorId;
using import "common.capnp".Resources;

interface ServerBootstrap {
    registerAsClient @0 (version :Int32) -> (service :ClientService);
    # Registers as a client, verifies the API version and returns the Client interface.

    registerAsGovernor @1 (version :Int32,
                         address :SocketAddress,
                         control: GovernorControl,
                         resources: Resources)
     -> (upstream :GovernorUpstream, governorId :GovernorId);
    # Registers as a governor, verifies the API version and returns the Governor upstream
    # interface (for calling the server with updates) and assigned governor id.
    # The `address` is the socket address with listening GovernorBootstrap interface.
    # If `address` is 0.0.0.0 or "::" (IPv6) (binding to all interfaces by
    # default), the server uses the peer address of the open connection.
}
