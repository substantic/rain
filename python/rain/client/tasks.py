from .task import Task
from .data import to_data
from .input import Input
from .output import Output
from .data import DataObject

import shlex


def concat(objs):
    """Creates a task concatenating data objects"""
    return Task("!concat", inputs=tuple(objs), outputs=1)


def sleep(timeout, dataobj):
    """Task that forwards argument 'dataobj' after 'timeout' seconds.
    The type of resulting data object is the same as type of input data object
    This task serves for testing purpose"""
    time_ms = int(timeout * 1000)
    dataobj = to_data(dataobj)
    return Task("!sleep",
                time_ms,
                inputs=(dataobj,),
                outputs=(dataobj.__class__("output"),))


def open(filename):
    return Task("!open", {"path": filename}, outputs=1)


def export(dataobj, filename):
    return Task("!export", {"path": filename}, inputs=(dataobj,))


def execute(args, stdout=None, stdin=None, input_files=(), output_files=(), shell=False):

    ins = []
    outs = []

    if stdout is not None:
        if stdout is True:
            stdout = "stdout"
        stdout = Output._for_program(stdout, label="stdout", execute=True)
        # '+out' is the file name of where stdout is redirected
        stdout.path = "+out"
        outs.append(stdout)

    if stdin is not None:
        # '+in' is the file name of where stdin is redirected
        stdin = Input._for_program(stdin, label="stdin", execute=True)
        stdin.path = "+in"
        ins.append(stdin)

    ins += [Input._for_program(obj, execute=True, label_as_path=True)
            for obj in input_files]
    outs += [Output._for_program(obj, execute=True, label_as_path=True)
             for obj in output_files]

    if isinstance(args, str):
        args = shlex.split(args)

    proc_args = []
    for i, a in enumerate(args):
        argname = "arg{}".format(i)
        if isinstance(a, str):
            proc_args.append(a)
        elif isinstance(a, Input) or isinstance(a, DataObject) or isinstance(a, Task):
            arg = Input._for_program(a, execute=True, label=argname)
            ins.append(arg)
            proc_args.append(arg.path)
        elif isinstance(a, Output):
            arg = Output._for_program(a, execute=True, label=argname)
            outs.append(arg)
            proc_args.append(arg.path)
        else:
            raise Exception("Argument {!r} is invalid".format(arg))

    if shell:
        proc_args = ("/bin/sh", "-c", " ".join(proc_args))
#        proc_args = ("/bin/sh", "-c", " ".join(shlex.quote(a) for a in proc_args))

    task_inputs = [obj.dataobj for obj in ins]
    task_outputs = [output.create_data_object() for output in outs]
    return Task("!run",
                {
                    "args": proc_args,
                    "in_paths": [obj.path for obj in ins],
                    "out_paths": [obj.path for obj in outs],
                },
                inputs=task_inputs,
                outputs=task_outputs)
