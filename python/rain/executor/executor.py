import os
import sys
import socket
import base64
import cloudpickle
import contextlib
import collections
import traceback

from ..common.fs import remove_dir_content
from ..common import DataInstance, RainException
from ..common.content_type import merge_content_types
from ..common.comm import SocketWrapper
from ..common.attributes import TaskSpec, ObjectSpec, ObjectInfo
from .context import Context


EXECUTOR_PROTOCOL = "cbor-1"


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
    input.spec.content_type = merge_content_types(input.content_type, content_type)
    if load:
        return input.load()
    else:
        return input


def load_governor_object(data, cache):
    spec = ObjectSpec._from_json(data["spec"])

    location = data["location"]
    if location == "cached":
        return cache[spec.id]

    path = None
    data = None
    if "memory" == location[0]:
        data = location[1]
    elif "path" in location[0]:
        path = location[1]
    else:
        raise Exception("Invalid location")

    data = DataInstance(data=data,
                        path=path,
                        data_type=spec.data_type,
                        spec=spec,
                        object_id=spec.id,
                        info=ObjectInfo())
    return data


def store_result(instance):

    if instance._object_id:
        location = ["other_object", instance._object_id]
    elif instance._path:
        location = ["path", instance._path]
    else:
        location = ["memory", instance._data]

    return {
        "info": instance.info._to_json() if instance.info else {},
        "location": location,
        "cache_hint": False,
    }


OutputSpec = collections.namedtuple(
    'OutputSpec', ['spec', 'encode'])


class Executor:

    def __init__(self, address, executor_id, task_path, stage_path):
        self.task_path = task_path
        self.stage_path = stage_path
        self.cache = {}

        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        # Protection against long filenames, socket names are limitted
        backup = os.getcwd()
        try:
            os.chdir(os.path.dirname(address))
            sock.connect(os.path.basename(address))
        finally:
            os.chdir(backup)

        self.socket = SocketWrapper(sock)
        self.socket.send_message(["register",
                                  {
                                      "protocol": EXECUTOR_PROTOCOL,
                                      "executor_id": executor_id,
                                      "executor_type": "py"
                                  }])

    def run(self):
        while True:
            message = self.socket.receive_message()
            self.process_message(message)

    def unpack_and_run_task(self, data):
        task_context = Context(self)
        task_context.spec = TaskSpec._from_json(data["spec"])
        try:
            inputs = []
            for dataobj in data["inputs"]:
                obj = load_governor_object(dataobj, self.cache)
                inputs.append(obj)

            self.cache[inputs[0]._object_id] = inputs[0]

            # List of OutputSpec
            outputs = [OutputSpec(
                spec=ObjectSpec._from_json(d["spec"]),
                encode=encode)
                for d, encode in zip(data["outputs"],
                                     task_context.spec.config['encode_outputs'])]

            del data  # We do not need reference to raw data anymore

            task_results = self.run_task(task_context, inputs, outputs)

            info = task_context.info
            if task_context._debug_messages:
                info.debug = "\n".join(task_context._debug_messages)

            self.socket.send_message(["result", {
                "task": task_context.spec.id,
                "success": True,
                "info": info._to_json(),
                "outputs": [store_result(data)
                            for data, output in zip(task_results, outputs)],
                "cached_objects": [inputs[0]._object_id],
            }])

        except Exception:
            task_context._cleanup_on_fail()

            info = task_context.info
            info.error = traceback.format_exc()
            if task_context._debug_messages:
                info.debug = "\n".join(task_context._debug_messages)

            self.socket.send_message(["result", {
                "task": task_context.spec.id,
                "info": info._to_json(),
                "success": False,
            }])

    def process_message(self, message):
        if message[0] == "call":
            self.unpack_and_run_task(message[1])
        elif message[0] == "drop_cached":
            for object_id in message[1]["objects"]:
                del self.cache[tuple(object_id)]
        else:
            raise Exception("Unknown message: {!r}".format(message[0]))

    def run_task(self, context, inputs, outputs):
        """
        Args:
            inputs: is a list of `DataInstance`.
            outputs: is list of `OutputSpec`.
        Returns:
            list(DataInstance)
        """
        fn = inputs[0].load(cache=True)
        context.function = fn
        cfg = context.spec.config
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
                raise RainException("No returned value allowed (0 outputs declared)")
            result = []
        if len(outputs) == 1:
            result = [result]
        if isinstance(result, collections.Mapping):
            result = [result.pop(o.spec.label) for o in outputs]
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
    executor_id = get_environ_int("RAIN_EXECUTOR_ID")

    print("Initalizing executor {} ...".format(executor_id))
    print("Working directory: ".format(os.getcwd()))
    sys.stdout.flush()

    os.makedirs("task")
    os.makedirs("stage")

    stage_path = os.path.abspath("stage")
    task_path = os.path.abspath("task")

    executor = Executor(get_environ("RAIN_EXECUTOR_SOCKET"),
                        executor_id,
                        task_path,
                        stage_path)
    print("Executor initialized")
    sys.stdout.flush()
    executor.run()
