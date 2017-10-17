import os
import sys
import capnp
import socket

from .rpc import subworker as rpc_subworker
from .control import ControlImpl

SUBWORKER_PROTOCOL_VERSION = 0


class Subworker:

    def __init__(self, address, subworker_id):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(address)
        self.rpc_client = capnp.TwoPartyClient(sock)

        upstream = self.rpc_client.bootstrap().cast_as(rpc_subworker.SubworkerUpstream)
        self.upstream = upstream

        control = ControlImpl(self)
        register = upstream.register_request()
        register.version = SUBWORKER_PROTOCOL_VERSION
        register.subworkerId = subworker_id
        register.subworkerType = "py"
        register.control = control
        register.send().wait()

    def run_task(self, config, inputs, outputs):
        fn = inputs[0].load(cache=True)
        result = fn(*inputs[1:])
        return self._decode_results(result, outputs)

    def _decode_results(self, result, outputs):
        if isinstance(result, bytes) and len(outputs) == 1:
            return [result]
        if isinstance(result, dict):
            return [result[label] for label in outputs]
        raise Exception("Invalid returned object:")


def get_environ(name):
    try:
        return os.environ[name]
    except KeyError:
        raise Exception("Env variable {} is not set".format(name))


def get_environ_int(name):
    try:
        return int(get_environ(name))
    except ValueError:
        raise Exception("Env variable {} is not valid integer".format(name))


def main():
    subworker_id = get_environ_int("RAIN_SUBWORKER_ID")

    print("Initalizing subworker {} ...".format(subworker_id))
    sys.stdout.flush()
    subworker = Subworker(get_environ("RAIN_SUBWORKER_SOCKET"), subworker_id)
    print("Subworker initialized")
    sys.stdout.flush()
    capnp.wait_forever()
