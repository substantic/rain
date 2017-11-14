import shlex

from .data import Blob
from .task import Task
from . import rpc


class Input:

    def __init__(self, label, path=None):
        self.label = label
        if path is None:
            path = label
        self.path = path


class Output:

    def __init__(self, label, path=None):
        self.label = label
        if path is None:
            path = label
        self.path = path


class Program:

    def __init__(self, args, stdout=None, stdin=None, vars=(), io=()):
        self.inputs = []
        self.outputs = []

        if stdout is not None:
            # +out is a name of where stdout is redirected
            self.outputs.append(Output(stdout, "+out"))

        if stdin is not None:
            # +in is a name of where stdin is redirected
            self.inputs.append(Input(stdin, "+in"))

        if isinstance(args, str):
            self.args = tuple(shlex.split(args))
        else:
            self.args = tuple(self._process_arg(arg) for arg in args)

        for obj in io:
            if isinstance(obj, Input):
                self.inputs.append(obj)
            elif isinstance(obj, Output):
                self.outputs.append(obj)
            else:
                raise Exception("Object {!r} is nor intput or output")

        self.vars = vars

    def _process_arg(self, arg):
        if isinstance(arg, str):
            return arg
        if isinstance(arg, Input):
            self.inputs.append(arg)
            return arg.path
        if isinstance(arg, Output):
            self.outputs.append(arg)
            return arg.path
        raise Exception("Argument {!r} is invalid".format(arg))

    def __repr__(self):
        return "<Program {}>".format(self.args)

    def __call__(self, **args):

        call_args = self.args
        for var in self.vars:
            var_string = "${{{}}}".format(var)
            call_args = [a.replace(var_string, args[var]) for a in call_args]

        config = rpc.tasks.RunTask.new_message()
        config.init("args", len(call_args))
        for i, arg in enumerate(call_args):
            config.args[i] = arg
        config.init("inputPaths", len(self.inputs))
        for i, obj in enumerate(self.inputs):
            config.inputPaths[i] = obj.path
        config.init("outputPaths", len(self.outputs))
        for i, obj in enumerate(self.outputs):
            config.outputPaths[i] = obj.path

        inputs = tuple(args[obj.label] for obj in self.inputs)
        # TODO: A proper error if there are too few or too many inputs

        outputs = [Blob(obj.label) for label in self.outputs]
        return Task("!run",
                    config.to_bytes_packed(),
                    inputs=inputs,
                    outputs=outputs)
