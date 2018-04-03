from .task import Task
from .data import to_data
from .input import Input, InputBase
from .output import Output, OutputBase, OutputDir
from .data import DataObject

import shlex


def concat(objs):
    """Creates a task concatenating data objects"""
    return Task("!concat", inputs=tuple(objs), outputs=1)


def sleep(timeout, dataobj, cpus=1):
    """Task that forwards argument 'dataobj' after 'timeout' seconds.
    The type of resulting data object is the same as type of input data object
    This task serves for testing purpose"""
    time_ms = int(timeout * 1000)
    dataobj = to_data(dataobj)
    return Task("!sleep",
                time_ms,
                inputs=(dataobj,),
                outputs=(dataobj.__class__("output"),),
                cpus=cpus)


def open(filename):
    return Task("!open", {"path": filename}, outputs=1)


def export(dataobj, filename):
    return Task("!export", {"path": filename}, inputs=(dataobj,))


def make_directory(dataobj_paths):
    paths = [path for path, dataobj in dataobj_paths]
    inputs = [to_data(dataobj) for path, dataobj in dataobj_paths]
    return Task("!make_directory", {"paths": paths}, inputs=inputs,
                outputs=(OutputDir("output"),))


def slice_directory(dataobj, path, content_type=None):
    if path.endswith("/"):
        c = OutputDir
    else:
        c = Output
    return Task("!slice_directory", {"path": path},
                inputs=(dataobj,),
                outputs=(c("output", content_type=content_type),))


def execute(args,
            stdout=None,
            stdin=None,
            input_paths=(),
            output_paths=(),
            shell=False,
            cpus=1):

    ins = []
    outs = []

    if stdout is not None:
        if stdout is True:
            stdout = "stdout"
        stdout = OutputBase._for_program(stdout, label="stdout", execute=True)
        # '+out' is the file name of where stdout is redirected
        stdout.path = "+out"
        outs.append(stdout)

    if stdin is not None:
        # '+in' is the file name of where stdin is redirected
        stdin = InputBase._for_program(stdin, label="stdin", execute=True)
        stdin.path = "+in"
        ins.append(stdin)

    ins += [InputBase._for_program(obj, execute=True, label_as_path=True)
            for obj in input_paths]
    outs += [OutputBase._for_program(obj, execute=True, label_as_path=True)
             for obj in output_paths]

    if isinstance(args, str):
        args = shlex.split(args)

    proc_args = []
    for i, a in enumerate(args):
        argname = "arg{}".format(i)
        if isinstance(a, str):
            proc_args.append(a)
        elif isinstance(a, InputBase) or isinstance(a, DataObject) or isinstance(a, Task):
            arg = Input._for_program(a, execute=True, label=argname)
            ins.append(arg)
            proc_args.append(arg.path)
        elif isinstance(a, OutputBase):
            arg = OutputBase._for_program(a, execute=True, label=argname)
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
                    "in_paths": [{"path": obj.path, "write": obj.write} for obj in ins],
                    "out_paths": [obj.path for obj in outs],
                },
                inputs=task_inputs,
                outputs=task_outputs,
                cpus=cpus)
