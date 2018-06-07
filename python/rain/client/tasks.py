from .task import Task
from .data import to_dataobj
from .input import Input, InputBase
from .output import Output, OutputBase, OutputDir
from .data import DataObject
from ..common.data_type import DataType
from ..common.utils import short_str

import shlex


class Concat(Task):
    """
    Creates a task concatenating the given data objects.
    """
    TASK_TYPE = "buildin/concat"

    def __init__(self, inputs, session=None):
        super().__init__(inputs, 1, session=session)


class Sleep(Task):
    """
    Task that forwards argument 'dataobj' after 'timeout' seconds.
    The type of resulting data object is the same as type of input data object
    This task serves for testing purposes.

    Args:
        input (`DataObject`): The object to pass through.
        timeout (`float`): Number of seconds to wait, converted to whole miliseconds.
        cpus (`int`): Number of CPUs to reserve, for testing purposes.
    """
    TASK_TYPE = "buildin/sleep"

    def __init__(self, input, timeout, *, session=None, cpus=1):
        input = to_dataobj(input)
        otype = Output if input.spec.data_type == DataType.BLOB else OutputDir
        output = otype(content_type=input.content_type)
        # , size_hint=input.spec.size_hint) TODO: Add size_hint
        super().__init__((input,), (output,), config=float(timeout), cpus=cpus, session=session)


class Load(Task):
    """
    Load and output a file at the given path (at the worker).
    """
    TASK_TYPE = "buildin/open"

    def __init__(self, path, output=None, *, session=None):
        if output is None:
            output = Output()
        output.expect_blob()

        super().__init__([], (output,), config={"path": path}, session=session)


class LoadDir(Task):
    """
    Load and output a directory at the given path (at the worker).

    TODO: Implement
    """
    TASK_TYPE = "buildin/open_dir"

    def __init__(self, path, output=None, *, session=None):
        if output is None:
            output = OutputDir()
        output.expect_dir()

        super().__init__([], OutputDir(), config={"path": path}, session=session)


class Store(Task):
    """
    Store the given object (blob or directory) at the given path (at the worker).
    """
    TASK_TYPE = "buildin/export"

    def __init__(self, input, path, *, session=None):
        super().__init__((input, ), 0, config={"path": path}, session=session)


class MakeDirectory(Task):
    """
    Create a directory from other objects (blobs or other directories).

    Args:
        paths_objects: An iterable of pairs `(path, obj)` or a dictionary `{path: obj}`
            where `path` is the new relative path. Paths of some of the more directory
            object may be `''` or `'.'` to use them as the base directory.

    TODO: Specify behavior on overlapping subdirs/contents.
    """
    TASK_TYPE = "buildin/make_directory"

    def __init__(self, paths_objects, *, session=None):
        if isinstance(paths_objects, dict):
            paths_objects = paths_objects.items()
        try:
            paths_objects = list(paths_objects)
            paths, inputs = zip(*paths_objects)
        except (TypeError, ValueError) as e:
            raise TypeError("MakeDirectory needs an iterable of pairs "
                            "`(path, obj)` or a dictionary `{path: obj}`") from e

        super().__init__(inputs, outputs=(OutputDir(),), config={"paths": paths}, session=session)


class SliceDirectory(Task):
    """Extract a file from a directory.

    Args:
        input (`DataObject`): A directory object to slice.
        path (`[str]`): An iterable of paths. If the path ends with a slash `'/'`, the output
            is a directory object, otherwise a file object is creates.
        output (`Output` or `OutputDir`): An optional output specification.
    """
    TASK_TYPE = "buildin/slice_directory"

    def __init__(self, input, path, output=None, *, session=None):
        input = to_dataobj(input)
        input.expect_dir()
        if output is None:
            if path.endswith('/'):
                output = OutputDir(path)
            else:
                output = Output(path)
        if path.endswith('/'):
            output.expect_dir()
        else:
            output.expect_blob()

        super().__init__((input,), (output,), config={"path": path}, session=session)


class Execute(Task):
    """
    A task executing a single external program with rich argument support.
    """
    TASK_TYPE = "buildin/run"

    def __init__(self, args,
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
#           proc_args = ("/bin/sh", "-c", " ".join(shlex.quote(a) for a in proc_args))

        task_inputs = [obj.dataobj for obj in ins]
        task_outputs = [output.create_data_object() for output in outs]
        config = {
            "args": proc_args,
            "in_paths": [{"path": obj.path, "write": obj.write} for obj in ins],
            "out_paths": [obj.path for obj in outs]}

        super().__init__(task_inputs, task_outputs, cpus=cpus, config=config)

    def __repr__(self):
        return "<{} {}, inputs {}, outputs {}, cmd {!r}>".format(
            self.__class__.__name__, self.id, self.inputs,
            self.outputs, short_str(self.spec.config["args"]))
