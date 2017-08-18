import os
import capnp
import socket
from rain.client import rpc

SUBWORKER_PROTOCOL_VERSION = 1

SRC_DIR = os.path.dirname(__file__)
capnp.remove_import_hook()
subworker_capnp = capnp.load(SRC_DIR + "/../../../capnp/subworker.capnp")


class Subworker:

    def __init__(self, address):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(address)
        self.rpc_client = capnp.TwoPartyClient(sock)

        upstream = self.rpc_client.bootstrap().cast_as(subworker_capnp.SubworkerUpstream)
        self.upstream = upstream
        registration = upstream.registerControl(SUBWORKER_PROTOCOL_VERSION).wait()


def get_environ(name):
    try:
        return os.environ[name]
    except KeyError:
        raise Exception("Environ variable {} is not set".format(name))


def main():
    subworker_id = get_environ("RAIN_SUBWORKER_ID")
    print("Initalizing subworker {} ...".format(subworker_id))
    subworker = Subworker(get_environ("RAIN_SUBWORKER_SOCKET"))
    print("Subworker initialized")

    while True:
        pass