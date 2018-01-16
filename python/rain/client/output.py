
from .data import DataObject
from ..common.content_type import check_content_type, merge_content_types
from copy import copy


class Output:
    """
    A multi-purpose object for specifying output data objects of tasks.

    May be used in task factory construction (e.g. in `@remote` and `Program`),
    or in concrete task instantiation (as `outputs=[...]` or `output=...`).

    A default label is the number of the output in the task.
    """

    def __init__(self, label=None, size_hint=None, content_type=None,
                 mode=None, encode=None, path=None):

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

    def to_json(self):
        return {k: v for (k, v) in self.__dict__.items() if v is not None}

    def _check_for_task(self, task, order):
        "Check the output for a task instance creation."
        if self.encode is not None or self.path is not None:
            raise ValueError("Task Outputs do not accept `encode`, `path`.")

    def _check_for_program(self, program):
        "Check and finalize the output for a Program factory."
        if self.encode is not None:
            raise ValueError("Program Outputs do not accept `encode`.")
        if self.path is None:
            self.path = "output_{}".format(self.label)

    def _check_for_remote(self, pytask):
        "Check and finalize the output for a Remote (pytask) factory."
        if self.path is not None:
            raise ValueError("Python remote task Outputs do not accept `path`.")

    def __repr__(self):
        if self.path is not None:
            return "<Output {} path={}>".format(self.label, self.path)
        else:
            return "<Output {}>".format(self.label)

    def merge_with_prototype(self, proto):
        "Return a copy of self updated with `Output` `proto` properties."
        assert isinstance(proto, Output)
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
        d = DataObject(label=self.label, session=session, content_type=self.content_type)
        if self.size_hint is not None:
            d.attributes['size_hint'] = self.size_hint
        return d


def to_output(obj):
    if isinstance(obj, Output):
        return obj
    if isinstance(obj, str):
        return Output(obj)
    raise Exception("Object {!r} cannot be used as output".format(obj))
