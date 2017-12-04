import os
import sys
import capnp
import socket
import pickle
import cloudpickle
import contextlib

from .rpc import subworker as rpc_subworker
from .control import ControlImpl
from ..common.fs import remove_dir_content

SUBWORKER_PROTOCOL_VERSION = 0


# List of input data objects while Py task arguments are unpickled.
# Not reentrant.
_global_unpickle_inputs = None


@contextlib.contextmanager
def _unpickle_inputs_context(inputs):
    """Context manager to store input data objects while Py task arguments
    are unpickled. Internal, not thread safe."""
    global _global_unpickle_inputs
    assert _global_unpickle_inputs is None
    _global_unpickle_inputs = inputs
    try:
        yield
    finally:
        _global_unpickle_inputs = None


def unpickle_input_object(name, index):
    """Helper to replace encoded input object placeholders with actual
    local data objects data."""
    global _global_unpickle_inputs
    assert _global_unpickle_inputs is not None
    return _global_unpickle_inputs[index]


class Subworker:

    def __init__(self, address, subworker_id, task_path, stage_path):
        self.task_path = task_path
        self.stage_path = stage_path
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(address)
        self.rpc_client = capnp.TwoPartyClient(sock)

        upstream = self.rpc_client.bootstrap().cast_as(
            rpc_subworker.SubworkerUpstream)
        self.upstream = upstream

        control = ControlImpl(self)
        register = upstream.register_request()
        register.version = SUBWORKER_PROTOCOL_VERSION
        register.subworkerId = subworker_id
        register.subworkerType = "py"
        register.control = control
        register.send().wait()

    def run_task(self, context, config, inputs, outputs):
        fn = inputs[0].load(cache=True)
        cfg = pickle.loads(config)
        with _unpickle_inputs_context(inputs):
            args = [cloudpickle.loads(d) for d in cfg["args"]]
            kwargs = dict((name, cloudpickle.loads(d))
                          for name, d in cfg["kwargs"].items())
        remove_dir_content(self.task_path)
        os.chdir(self.task_path)
        result = fn(context, *args, **kwargs)
        # TODO:(gavento) Handle `cfg['outputs']` and `cfg['pickle_outputs']`
        return self._decode_results(result, outputs)

    def _decode_results(self, result, outputs):
        if isinstance(result, dict):
            return [result[label] for label in outputs]
        if len(outputs) == 1:
            return [result]
        raise Exception("Invalid result of task:" + repr(result))


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
    print("Working directory: ".format(os.getcwd()))
    sys.stdout.flush()

    os.makedirs("task")
    os.makedirs("stage")

    stage_path = os.path.abspath("stage")
    task_path = os.path.abspath("task")

    Subworker(get_environ("RAIN_SUBWORKER_SOCKET"),
              subworker_id,
              task_path,
              stage_path)

    print("Subworker initialized")
    sys.stdout.flush()
    capnp.wait_forever()
