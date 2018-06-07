
import collections
from copy import copy

from ..common.content_type import check_content_type, merge_content_types
from ..common.labeled_list import LabeledList
from .data import DataObject, DataType


class OutputBase:
    """
    A multi-purpose object for specifying output data objects of tasks.

    May be used in task factory construction (e.g. in `@remote` and `Program`),
    or in concrete task instantiation (as `outputs=[...]` or `output=...`).

    A default label is the number of the output in the task.
    """

    data_type = None

    def __init__(self, label=None, *, size_hint=None, content_type=None,
                 mode=None, encode=None, path=None):
        assert self.data_type is not None
        self.label = label
        self.size_hint = size_hint
        self.content_type = content_type
        check_content_type(self.content_type)
        assert mode is None, "Data object modes not supported yet"
        self.encode = encode
        if (self.encode is not None and self.content_type is not None and
           self.content_type != self.encode and self.content_type != ""):
            raise ValueError(
                "When specifying both encode and content_type " +
                "for Output, they must match.")

        self.path = path

    def _to_json(self):
        return {k: v for (k, v) in self.__dict__.items() if v is not None}

    def _check_for_task(self, task, order):
        "Check the output for a task instance creation."
        if self.encode is not None or self.path is not None:
            raise ValueError("Task Outputs do not accept `encode`, `path`.")

    def _check_for_remote(self, pytask):
        "Check and finalize the output for a Remote (pytask) factory."
        if self.path is not None:
            raise ValueError("Python remote task Outputs do not accept `path`.")

    def merge_with_prototype(self, proto):
        "Return a copy of self updated with `Output` `proto` properties."
        assert isinstance(proto, OutputBase)
        o = copy(self)
        if o.size_hint is None:
            o.size_hint == proto.size_hint
        if o.label is None:
            o.label = proto.label
        if o.path is None:
            o.path = proto.path
        o.content_type = merge_content_types(o.content_type, proto.content_type)
        o.encode = merge_content_types(o.encode, proto.encode)
        return o

    def create_data_object(self, session=None):
        d = DataObject(label=self.label,
                       session=session,
                       data_type=self.data_type,
                       content_type=self.content_type)
        if self.size_hint is not None:
            d.spec.size_hint = self.size_hint
        return d

    @classmethod
    def _for_program(cls, out, label=None, execute=False, label_as_path=False):
        """
        Create `Output` from `Output` or `str` for `Program` or `execute`.
        """
        if isinstance(out, str):
            out = Output(out)
        if not isinstance(out, OutputBase):
            raise TypeError("Object {!r} cannot be used as output".format(out))
        if out.label is None:
            out.label = label
        if out.label is None:
            raise ValueError("Program/execute Outputs need `label`")
        if out.encode is not None:
            raise ValueError("Program/execute Outputs do not accept `encode`.")

        if execute and out.path is None:
            if label_as_path:
                out.path = out.label
            else:
                out.path = "out_{}".format(out.label)

        return out

    def expect_dir(self):
        """Raise TypeError if the Output is not a directory data-type."""
        if self.data_type != DataType.DIRECTORY:
            raise TypeError("Directory output object expected.")

    def expect_blob(self):
        """Raise TypeError if the Output is not a directory data-type."""
        if self.data_type != DataType.BLOB:
            raise TypeError("Directory output object expected.")


class Output(OutputBase):
    data_type = DataType.BLOB

    def __repr__(self):
        if self.path is not None:
            return "<Output {!r} path={!r}>".format(self.label, self.path)
        else:
            return "<Output {!r}>".format(self.label)


class OutputDir(OutputBase):
    data_type = DataType.DIRECTORY

    def __repr__(self):
        if self.path is not None:
            return "<OutputDir {!r} path={!r}>".format(self.label, self.path)
        else:
            return "<OutputDir {!r}>".format(self.label)


def to_output(obj):
    if isinstance(obj, OutputBase):
        return obj
    if isinstance(obj, str):
        return Output(obj)
    raise Exception("Object {!r} cannot be used as output".format(obj))


class OutputSpec:
    """
    A base class for task outputs list.
    Provides input and output specification, checking and instantiation.
    """

    # Required / default outputs; LabeledList of `Output`s
    outputs = ()

    def __init__(self, outputs=None, output=None):

        if output is not None:
            if outputs is not None:
                raise ValueError("Both `output` and `outputs` not allowed.")
            outputs = (output,)

        if isinstance(outputs, int):
            self.outputs = LabeledList(Output() for i in range(outputs))
        elif isinstance(outputs, LabeledList):
            self.outputs = outputs
        elif isinstance(outputs, collections.Sequence):
            self.outputs = LabeledList(outputs)
        else:
            raise TypeError("expected int, LabeledList or a sequence "
                            "for `outputs`, got {:r}".format(type(outputs)))

        for i, (label, output) in enumerate(self.outputs.items()):
            if isinstance(output, str):
                self.outputs.set(i, Output(label=output), label=output)
            elif not isinstance(output, OutputBase):
                raise TypeError("Only string labels and `Output` accepted in output list.")
        # Config for auto-encoding
        self.encode = [None] * len(self.outputs)

    def instantiate(self, outputs=None, output=None, session=None):
        """
        Create new output `DataObject`s for `Output`s given.

        Returns a tuple of `LabeledList`s `(outputs, data_objects)`.
        If both `output=None` and `outputs=None`, creates builder prototype outputs.
        """

        if output is not None:
            if outputs is not None:
                raise ValueError("Both `output` and `outputs` not allowed.")
            outputs = (output,)

        if outputs is None:
            outputs = LabeledList(self.outputs)
        if not isinstance(outputs, LabeledList):
            if not isinstance(outputs, collections.Sequence):
                raise TypeError("`outputs` must be None or a sequence type.")
            outputs = LabeledList(outputs)

        if len(outputs) != len(self.outputs):
            raise ValueError("Got {} outputs, {} expected."
                             .format(len(outputs), len(self.outputs)))

        objs = LabeledList()
        self.encode = [None] * len(outputs)
        for i, (label, out) in enumerate(outputs.items()):
            if i < len(self.outputs):
                proto = self.outputs[i]
            else:
                proto = self.more_outputs
            if isinstance(out, str):
                out = Output(label=out)
            if out is None:
                out = Output()
            if not isinstance(out, OutputBase):
                raise TypeError("Only `Output` and `str` instances accepted in output list.")
            out_merged = out.merge_with_prototype(proto)
            if out_merged.label is None:
                out_merged.label = "out{}".format(i)
            do = out_merged.create_data_object(session=session)
            if out_merged.encode is not None:
                self.encode[i] = out_merged.encode
                do._spec.content_type = out_merged.encode
            if out_merged.size_hint is not None:
                do._spec.size_hint = out_merged.size_hint
            objs.append(do, label=do._spec.label)

        return objs
