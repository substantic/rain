from .task import Task
from .data import to_data
from .input import Input, to_input_with_data
from .output import Output, to_output
from .data import DataObject

import struct
import shlex
from . import rpc


def concat(objs):
    """Creates a task that Concatenate data objects"""
    return Task("!concat", inputs=tuple(objs), outputs=1)


def sleep(timeout, dataobj):
    """Task that forwards argument 'dataobj' after 'timeout' seconds.
    The type of resulting data object is the same as type of input data object
    This task serves for testing purpose"""
    time_ms = int(timeout * 1000)
    dataobj = to_data(dataobj)
    return Task("!sleep",
                struct.pack("<I", time_ms),
                inputs=(dataobj,),
                outputs=(dataobj.__class__("output"),))


def open(filename):
    return Task("!open", filename, outputs=1)


def execute(args, stdout=None, stdin=None, inputs=(), outputs=()):

    ins = []
    outs = []

    def process_arg(arg):
        if isinstance(arg, str):
            return arg
        if isinstance(arg, Input) \
           or isinstance(arg, DataObject) or isinstance(arg, Task):
            arg = to_input_with_data(arg)
            ins.append(arg)
            return arg.path
        if isinstance(arg, Output):
            outs.append(arg)
            return arg.path
        raise Exception("Argument {!r} is invalid".format(arg))

    if isinstance(args, str):
        args = shlex.split(args)
    else:
        args = [process_arg(arg) for arg in args]

    if stdout is not None:
        if stdout is True:
            stdout = "stdout"
        stdout = to_output(stdout)
        # '+out' is a name of where stdout is redirected
        stdout.path = "+out"
        outs.append(stdout)

    if stdin is not None:
        # '+in' is a name of where stdin is redirected
        stdin = to_input_with_data(stdin, "stdin")
        stdin.path = "+in"
        ins.append(stdin)

    ins += [to_input_with_data(obj) for obj in inputs]
    outs += [to_output(obj) for obj in outputs]

    config = rpc.tasks.RunTask.new_message()
    config.init("args", len(args))
    for i, arg in enumerate(args):
        config.args[i] = arg
    config.init("inputPaths", len(ins))
    for i, obj in enumerate(ins):
        config.inputPaths[i] = obj.path
    config.init("outputPaths", len(outs))
    for i, obj in enumerate(outs):
        config.outputPaths[i] = obj.path

    task_inputs = [obj.data for obj in ins]
    task_outputs = [output.make_data_object() for output in outs]
    print(ins, outs)
    return Task("!run",
                config.to_bytes_packed(),
                inputs=task_inputs,
                outputs=task_outputs)
