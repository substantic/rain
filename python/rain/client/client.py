import capnp
import os

CLIENT_PROTOCOL_VERSION = 0

SRC_DIR = os.path.dirname(__file__)
capnp.remove_import_hook()
server_capnp = capnp.load(SRC_DIR + "/../../../capnp/server.capnp")


class Client:

    def __init__(self, address, port):
        self.submit_id = 0
        self.handles = {}
        self.rpc_client = capnp.TwoPartyClient("{}:{}".format(address, port))

        bootstrap = self.rpc_client.bootstrap().cast_as(server_capnp.ServerBootstrap)
        registration = bootstrap.registerAsClient(CLIENT_PROTOCOL_VERSION)
        self.service = registration.wait().service

    def get_server_info(self):
        """ Returns basic server info """
        info = self.service.getServerInfo().wait()
        return {
            "n_workers": info.nWorkers
        }
