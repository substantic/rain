import shlex
from copy import copy

from .input import Input
from .output import Output
from .tasks import Execute


class Program:
    # Input filenames
    input_paths = ()
    # Output filenames
    output_paths = ()
    stdin = None
    stdout = None
    shell = False

    def __init__(self,
                 args,
                 stdout=None, stdin=None,
                 input_paths=(), output_paths=(),
                 shell=False,
                 cpus=1):

        if stdin is not None:
            self.stdin = Input._for_program(stdin, label="stdin")

        if stdout:
            if stdout is True:
                stdout = "stdout"
            self.stdout = Output._for_program(stdout, label="stdout")

        self.input_paths = tuple(Input._for_program(obj, label_as_path=True)
                                 for obj in input_paths)
        self.output_paths = tuple(Output._for_program(obj, label_as_path=True)
                                  for obj in output_paths)
        self.cpus = cpus

        if isinstance(args, str):
            args = shlex.split(args)
        self.args = []
        for i, a in enumerate(args):
            if isinstance(a, str):
                self.args.append(a)
            elif isinstance(a, Input):
                if a.label is None:
                    a.label = "arg{}".format(i)
                self.args.append(Input._for_program(a))
            elif isinstance(a, Output):
                if a.label is None:
                    a.label = "arg{}".format(i)
                self.args.append(Output._for_program(a))
            else:
                raise TypeError("Can't use {!r} in program argument list."
                                .format(a))

        self.shell = shell

    def __repr__(self):
        return "<Program {}>".format(self.args)

    def __call__(self, **kw):
        def apply_data(obj):
            if isinstance(obj, Input):
                new = copy(obj)
                new.dataobj = kw[obj.label]
                return new
            else:
                return obj

        return Execute([apply_data(obj) for obj in self.args],
                       stdout=self.stdout,
                       stdin=apply_data(self.stdin),
                       input_paths=[apply_data(obj) for obj in self.input_paths],
                       output_paths=[obj for obj in self.output_paths],
                       shell=self.shell,
                       cpus=self.cpus)
