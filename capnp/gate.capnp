@0xb01bcb96f4bd00be;

using ClientService = import "client.capnp".ClientService;

interface Gate {
  registerAsClient @0 (version :Int32) -> (service :ClientService);
}
