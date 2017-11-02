import shlex

from .data import Blob
from .task import Task
from . import rpc


class Program:

    def __init__(self, args, stdout=None, stdin=None, vars=()):
        if isinstance(args, str):
            args = tuple(shlex.shlex(args))
        self.args = args

        self.output_paths = []
        self.output_labels = []

        self.input_paths = []
        self.input_labels = []

        if stdout is not None:
            # +out is a name of where stdout is redirected
            self.output("+out", stdout)

        if stdin is not None:
            # +in is a name of where stdout is redirected
            self.input("+in", stdin)

        self.vars = vars

    def input(self, path, label=None):
        """Create new input"""
        if label is None:
            label = path
        self.input_paths.append(path)
        self.input_labels.append(label)
        return self

    def output(self, path, label=None):
        """Create new output"""
        if label is None:
            label = path
        self.output_paths.append(path)
        self.output_labels.append(label)
        return self

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
        config.init("inputPaths", len(self.input_paths))
        for i, path in enumerate(self.input_paths):
            config.inputPaths[i] = path
        config.init("outputPaths", len(self.output_paths))
        for i, path in enumerate(self.output_paths):
            config.outputPaths[i] = path

        inputs = tuple(args[label] for label in self.input_labels)
        # TODO: A proper error if there are too few or too many inputs

        outputs = [Blob(label) for label in self.output_labels]
        return Task("!run",
                    config.to_bytes_packed(),
                    inputs=inputs,
                    outputs=outputs)
