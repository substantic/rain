import shlex

from .data import Blob
from .task import Task
from . import rpc

class Program:

    def __init__(self, args, inputs=(), outputs=(), stdout=None):
        if isinstance(args, str):
            args = tuple(shlex.shlex(args))
        self.args = args

        self.output_paths = tuple(o[0] for o in outputs)
        self.output_labels = tuple(o[1] for o in outputs)

        self.input_paths = tuple(o[0] for o in inputs)
        self.input_labels = tuple(o[1] for o in inputs)

        if stdout is not None:
            self.output_paths += ("+out",)
            self.output_labels += (stdout,)

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
