import os
import sys
import capnp
import socket
import base64
import cloudpickle
import contextlib
import collections

from .rpc import subworker as rpc_subworker
from .control import ControlImpl
from ..common.fs import remove_dir_content
from ..common import DataInstance, RainException
from ..common.content_type import merge_content_types

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


def unpickle_input_object(name, index, load, content_type):
    """Helper to replace encoded input object placeholders with actual
    local data objects data."""
    global _global_unpickle_inputs
    assert _global_unpickle_inputs is not None
    input = _global_unpickle_inputs[index]
    input.attributes['spec']['content_type'] = \
        merge_content_types(input.content_type, content_type)
    if load:
        return input.load()
    else:
        return input


class Subworker:

    def __init__(self, address, subworker_id, task_path, stage_path):
        self.task_path = task_path
        self.stage_path = stage_path
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        # Protection against long filenames, socket names are limitted
        backup = os.getcwd()
        try:
            os.chdir(os.path.dirname(address))
            sock.connect(os.path.basename(address))
        finally:
            os.chdir(backup)

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

    def run_task(self, context, inputs, outputs):
        """
        Args:
            inputs: is a list of `DataInstance`.
            outputs: is list of `ControlImpl.OutputSpec`.
        Returns:
            list(DataInstance)
        """
        fn = inputs[0].load(cache=True)
        cfg = context.attributes["config"]
        with _unpickle_inputs_context(inputs):
            args = [cloudpickle.loads(base64.b64decode(d))
                    for d in cfg["args"]]
            kwargs = dict((name, cloudpickle.loads(base64.b64decode(d)))
                          for name, d in cfg["kwargs"].items())
        remove_dir_content(self.task_path)
        os.chdir(self.task_path)

        # Run the function
        result = fn(context, *args, **kwargs)

        if len(outputs) == 0:
            if result is not None and result != ():
                raise RainException("No returned value allowed (0 outputs declared")
            result = []
        if len(outputs) == 1:
            result = [result]
        if isinstance(result, collections.Mapping):
            result = [result.pop(o.label) for o in outputs]
        if not isinstance(result, collections.Sequence):
            raise RainException("Invalid result of task (not a sequence type): {!r}"
                                .format(result))
        if len(result) != len(outputs):
            raise RainException("Python task should return {} outputs, got {}."
                                .format(len(outputs), len(result)))
        res = []
        for r, o in zip(result, outputs):
            encode = o.encode
            if isinstance(r, DataInstance):
                di = r
            elif encode is not None:
                di = context.blob(r, encode=encode)
            elif isinstance(r, str):
                di = context.blob(r, encode="text")
            elif isinstance(r, bytes):
                di = context.blob(r)
            else:
                raise Exception("Invalid result object: {!r}".format(r))
            di.attributes['spec'] = o.attributes['spec']
            if 'user_spec' in o.attributes:
                di.attributes['user_spec'] = o.attributes['user_spec']
            res.append(di)

        return res


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
