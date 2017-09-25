import shlex

from .data import Blob
from .task import Task
from . import rpc


class Program:

    def __init__(self, args, stdout=None, stdin=None):
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


    def input(self, path, label):
        """Create new input"""
        self.input_paths.append(path)
        self.input_labels.append(label)

    def output(self, path, label):
        """Create new output"""
        self.output_paths.append(path)
        self.output_labels.append(label)

    def __repr__(self):
        return "<Program {}>".format(self.args)

    def __call__(self, **args):
        config = rpc.tasks.RunTask.new_message()
        config.init("args", len(self.args))
        for i, arg in enumerate(self.args):
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
        return Task("run",
                    config.to_bytes_packed(),
                    inputs=inputs,
                    outputs=outputs)
