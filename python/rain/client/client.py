import capnp
import os

CLIENT_PROTOCOL_VERSION = 0

SRC_DIR = os.path.dirname(__file__)
capnp.remove_import_hook()
gate_capnp = capnp.load(SRC_DIR + "/../../../capnp/gate.capnp")


class Client:

    def __init__(self, address, port):
        self.submit_id = 0
        self.handles = {}
        self.rpc_client = capnp.TwoPartyClient("{}:{}".format(address, port))

        gate = self.rpc_client.bootstrap().cast_as(gate_capnp.Gate)
        registration = gate.registerAsClient(CLIENT_PROTOCOL_VERSION)
        self.service = registration.wait().service

    def get_info(self):
        """ Returns basic server info """
        info = self.service.getInfo().wait()
        return {
            "n_workers": info.nWorkers
        }
