import os
import sys
import json
import socket
import base64
import cloudpickle
import contextlib
import collections

from ..common.fs import remove_dir_content
from ..common import DataInstance, RainException
from ..common.content_type import merge_content_types
from ..common.comm import SocketWrapper
from .context import Context
from ..common.datatype import DataType


SUBWORKER_PROTOCOL = "xxx"


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


def load_attributes(data):
    return dict((k, json.loads(v)) for k, v in data.items())


def store_attributes(attributes):
    return dict((k, json.dumps(v)) for k, v in attributes.items())


def load_worker_object(data, cache):
    object_id = tuple(data["id"])
    attributes = load_attributes(data["attributes"])

    location = data["location"]
    path = None
    data = None
    if "memory" in location:
        data = bytes(location["memory"])  # TODO: remove bytes
    elif "path" in location:
        path = location["path"]

    # TODO: data type
    data = DataInstance(data=data, path=path, attributes=attributes, data_type=DataType.BLOB)
    data._object_id = object_id
    return data


def store_result(instance, id):

    if instance._object_id:
        location = {"objectData": instance._object_id}
    elif instance._path:
        location = {"path": instance._path}
    else:
        location = {"memory": list(instance._data)}  # TODO: remove list

    return {
        "id": id,
        "attributes": store_attributes(instance.attributes),
        "location": location,
        "cacheHint": False,
    }


OutputSpec = collections.namedtuple(
    'OutputSpec', ['label', 'id', 'encode', 'attributes'])


class Subworker:

    def __init__(self, address, subworker_id, task_path, stage_path):
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
        self.socket.send_message({"message": "register",
                                  "data": {
                                    "protocol": SUBWORKER_PROTOCOL,
                                    "subworkerId": subworker_id,
                                    "subworkerType": "py"}})

    def run(self):
        while True:
            message = self.socket.receive_message()
            self.process_message(message)

    def unpack_and_run_task(self, data):
        task_context = Context(self, tuple(data["task"]))

        class XXX(Exception):
            pass

        try:
            task_context.attributes = load_attributes(data["attributes"])
            cfg = task_context.attributes["config"]

            inputs = []
            for dataobj in data["inputs"]:
                obj = load_worker_object(dataobj, self.cache)
                #TODO if reader.saveInCache:
                #TODO    self.cache[obj._object_id] = obj
                inputs.append(obj)

            # List of OutputSpec
            outputs = [OutputSpec(
                            label=d.get("label"),
                            id=tuple(d["id"]),
                            attributes=load_attributes(d["attributes"]),
                            encode=encode)
                       for d, encode in zip(data["outputs"],
                                            cfg['encode_outputs'])]

            del data  # We do not need reference to raw data anymore

            task_results = self.run_task(task_context, inputs, outputs)
            self.socket.send_message({"message": "result", "data": {
                "task": task_context.task_id,
                "success": True,
                "attributes": store_attributes(task_context.attributes),
                "outputs": [store_result(data, output.id) for data, output in zip(task_results, outputs)]
            }})

            #results = _context.results.init("data", len(task_results))
            #for i, data in enumerate(task_results):
            #    data._to_capnp(results[i])
            #task_context._cleanup(task_results)
            #write_attributes(task_context, _context.results.taskAttributes)
            #_context.results.ok = True

        except XXX:
            task_context._cleanup_on_fail()
            _context.results.errorMessage = traceback.format_exc()
            write_attributes(task_context, _context.results.taskAttributes)
            _context.results.ok = False

    def process_message(self, message):
        if message["message"] == "call":
            self.unpack_and_run_task(message["data"])
        else:
            raise Exception("Unknown message")
        sys.stdout.flush()
        pass  # TODO

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
                raise RainException("No returned value allowed (0 outputs declared)")
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

    subworker = Subworker(get_environ("RAIN_SUBWORKER_SOCKET"),
                          subworker_id,
                          task_path,
                          stage_path)
    print("Subworker initialized")
    sys.stdout.flush()
    subworker.run()

