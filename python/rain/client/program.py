import shlex

from .tasks import execute
from .input import Input, to_input
from .output import to_output

from copy import copy


class Program:

    def __init__(self,
                 args,
                 stdout=None, stdin=None,
                 inputs=(), outputs=(),
                 shell=False):

        def check_arg(obj):
            if isinstance(obj, Input) and obj.data is not None:
                raise Exception("Input used in Program cannot have data")

        if isinstance(args, str):
            args = shlex.split(args)

        self.args = tuple(args)
        if stdin:
            stdin = to_input(stdin)
        if stdout:
            if stdout is True:
                stdout = "stdout"
            stdout = to_output(stdout)
        self.stdin = stdin
        self.stdout = stdout
        self.inputs = tuple(to_input(obj) for obj in inputs)
        self.outputs = tuple(to_output(obj) for obj in outputs)

        for obj in args:
            check_arg(obj)

        self.shell = shell

    def __repr__(self):
        return "<Program {}>".format(self.args)

    def __call__(self, **kw):
        def apply_data(obj):
            if isinstance(obj, Input):
                new = copy(obj)
                new.data = kw[obj.label]
                return new
            else:
                return obj
        return execute([apply_data(obj) for obj in self.args],
                       stdout=self.stdout,
                       stdin=apply_data(self.stdin),
                       inputs=[apply_data(obj) for obj in self.inputs],
                       outputs=[apply_data(obj) for obj in self.outputs],
                       shell=self.shell)
