import shlex

from .tasks import execute
from .input import Input
from .output import Output

from copy import copy


class Program:
    # Input filenames
    input_files = ()
    # Output filenames
    output_files = ()
    stdin = None
    stdout = None
    shell = False

    def __init__(self,
                 args,
                 stdout=None, stdin=None,
                 input_files=(), output_files=(),
                 shell=False,
                 cpus=1):

        if stdin is not None:
            self.stdin = Input._for_program(stdin, label="stdin")

        if stdout:
            if stdout is True:
                stdout = "stdout"
            self.stdout = Output._for_program(stdout, label="stdout")

        self.input_files = tuple(Input._for_program(obj, label_as_path=True)
                                 for obj in input_files)
        self.output_files = tuple(Output._for_program(obj, label_as_path=True)
                                  for obj in output_files)
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
                self.args.append(Input._for_program(a))
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

        return execute([apply_data(obj) for obj in self.args],
                       stdout=self.stdout,
                       stdin=apply_data(self.stdin),
                       input_files=[apply_data(obj) for obj in self.input_files],
                       output_files=[obj for obj in self.output_files],
                       shell=self.shell,
                       cpus=self.cpus)
