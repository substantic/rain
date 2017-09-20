from .task import Task
from .data import to_data
from . import rpc
import struct
import shlex


def concat(*objs):
    """Creates a task that Concatenate data objects"""
    return Task("concat", inputs=objs)


def sleep(timeout, dataobj):
    """Task that forwards argument 'dataobj' after 'timeout' seconds.
    The type of resulting data object is the same as type of input data object
    This task serves for testing purpose"""
    time_ms = int(timeout * 1000)
    dataobj = to_data(dataobj)
    return Task("sleep",
                struct.pack("<I", time_ms),
                inputs=(dataobj,),
                outputs=(dataobj.__class__("output"),))


class Program:

    def __init__(self, args, inputs=(), outputs=()):
        if isinstance(args, str):
            args = tuple(shlex.shlex(args))
        self.args = args
        self.inputs = tuple(inputs)
        self.outputs = tuple(outputs)

    def __repr__(self):
        return "<Program {}>".format(self.args)

    def __call__(self, **args):
        config = rpc.tasks.RunTask.new_message()
        config.init("args", len(self.args))
        for i, arg in enumerate(self.args):
            config.args[i] = arg
        return Task("run", config.to_bytes_packed(), inputs=(), outputs=())