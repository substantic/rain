from .session import get_active_session
from .data import DataObject, to_data
from ..common import RainException
from .output import Output
from .input import Input
from ..common.content_type import is_type_instance, merge_content_types
from ..common import LabeledList
from ..common.resources import cpu_1
from ..common.attributes import attributes_to_capnp

import collections


class TaskBuilder:
    """
    A base class for Task builders (tasks.*, Program, @remote).

    Provides input and output specification, checking and instantiation.
    """

    # Required / default inputs; LabeledList of `Input`s
    inputs = ()
    # An `Input` instance if additional inputs are allowed (as a template)
    more_inputs = None
    # Required / default outputs; LabeledList of `Output`s
    outputs = ()
    # An `Output` instance if additional outputs are allowed (as a template)
    more_outputs = None

    def __init__(self, inputs=(), more_inputs=False, outputs=1, more_outputs=False):

        if isinstance(inputs, int):
            self.inputs = LabeledList(input() for i in range(inputs))
        elif isinstance(inputs, LabeledList):
            self.inputs = inputs
        elif isinstance(inputs, collections.Sequence):
            self.inputs = LabeledList(inputs)
        else:
            raise TypeError("expected int, LabeledList or a sequence " +
                            "for `inputs`, got {:r}".format(type(inputs)))

        for i, (label, input) in enumerate(self.inputs.items()):
            if isinstance(input, str):
                self.inputs.set(i, Input(input), label=input)
            elif not isinstance(input, Input):
                raise TypeError("Only string labels and `Input` accepted in input list.")

        if more_inputs is not None:
            if more_inputs is True:
                more_inputs = Input()
            if not isinstance(more_inputs, Input):
                raise TypeError("None, True or Input accepted for more_inputs.")
        self.more_inputs = more_inputs

        if isinstance(outputs, int):
            self.outputs = LabeledList(Output() for i in range(outputs))
        elif isinstance(outputs, LabeledList):
            self.outputs = outputs
        elif isinstance(outputs, collections.Sequence):
            self.outputs = LabeledList(outputs)
        else:
            raise TypeError("expected int, LabeledList or a sequence " +
                            "for `outputs`, got {:r}".format(type(outputs)))

        for i, (label, output) in enumerate(self.outputs.items()):
            if isinstance(output, str):
                self.outputs.set(i, Output(output), label=output)
            elif not isinstance(output, Output):
                raise TypeError("Only string labels and `Output` accepted in output list.")

        if more_outputs is not None:
            if more_outputs is True:
                more_outputs = Output()
            if not isinstance(more_outputs, Output):
                raise TypeError("None, True or Output accepted for more_outputs.")
        self.more_outputs = more_outputs

    def create_inputs(self, inputs):
        """
        Create input instances with the `DataObject`s and `Input`s given and return a `LabeledList`.

        The types are checked, labels are obtained from the `given` labels, `Input` labels,
        `DataObject` labels, prototype `Input` labels or just numbered as `"in{}"` (in that order).
        """

        if inputs is None:
            inputs = LabeledList()
        if len(inputs) < len(self.inputs):
            raise ValueError("Only {} of expected {} inputs provided."
                             .format(len(inputs), len(self.inputs)))
        if len(inputs) > len(self.inputs) and self.more_inputs is None:
            raise ValueError("Too many inputs provided {}, expected {}."
                             .format(len(inputs), len(self.inputs)))

        res = LabeledList()
        for i, (label, input) in enumerate(inputs.items()):
            if isinstance(input, Input):
                if not isinstance(input.data, DataObject):
                    raise TypeError("All inputs must be, or have DataObject instances.")
                do = input.data
                if label is None:
                    label = input.label
            elif isinstance(input, DataObject):
                do = input
            else:
                raise TypeError("Only Input and DataObject instances accepted in input list.")
            if label is None:
                label = do.label
            if i < len(self.inputs):
                proto = self.inputs[i]
                if label is None:
                    label = proto.label
            else:
                proto = self.more_inputs
            if label is None:
                label = "in{}".format(i)
            if not is_type_instance(do.content_type, proto.content_type):
                raise RainException("Input {!r} type {!r} is not a subtype of expected {!r}"
                                    .format(label, do.content_type, proto.content_type))
            res.append(do, label=label)

        return res

    def create_outputs(self, outputs=None, output=None, session=None):
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

        if len(outputs) < len(self.outputs):
            raise ValueError("Only {} of expected {} outputs provided."
                             .format(len(outputs), len(self.outputs)))
        if len(outputs) > len(self.outputs) and self.more_outputs is None:
            raise ValueError("Too many outputs provided {}, expected {}."
                             .format(len(outputs), len(self.outputs)))

        res_do = LabeledList()
        res_out = LabeledList()
        for i, (label, out) in enumerate(outputs.items()):
            if i < len(self.outputs):
                proto = self.outputs[i]
            else:
                proto = self.more_outputs
            if isinstance(out, str):
                out = Output(label=out)
            if not isinstance(out, Output):
                raise TypeError("Only `Output` oad `str` instances accepted in output list.")
            out_merged = out.merge_with_prototype(proto)
            if out_merged.label is None:
                out_merged.label = "out{}".format(i)
            do = out_merged.create_data_object(session=session)
            res_do.append(do, label=do.label)
            res_out.append(out_merged, label=do.label)
        return (res_out, res_do)


class Task:

    # State of object
    # None = Not submitted
    state = None
    id = None
    resources = cpu_1
    config = None


    def __init__(self,
                 task_type,
                 config=None,
                 inputs=(),
                 outputs=None,
                 session=None):
        if session is None:
            session = get_active_session()
        self.session = session
        self.id = session._register_task(self)

        self.task_type = task_type
        self.attributes = {}

        if config is not None:
            self.attributes["config"] = config

        if outputs is None:
            outputs = ()
        elif isinstance(outputs, int):
            outputs = tuple(DataObject(session=session)
                            for i in range(outputs))
        else:
            outputs = tuple(DataObject(obj, session=session)
                            if isinstance(obj, str)
                            else obj for obj in outputs)

        self.outputs = LabeledList(pairs=((output.label, output)
                                          for output in outputs))

        input_pairs = []
        for input in inputs:
            if isinstance(input, tuple):
                label, inp = input
                input_pairs.append((label, to_data(inp)))
            else:
                input_pairs.append((None, to_data(input)))
        self.inputs = LabeledList(pairs=input_pairs)

    def keep_outputs(self):
        """Keep all outputs of the task"""
        for output in self.outputs:
            output.keep()

    def unkeep_outputs(self):
        """Unkeep all outputs of the task"""
        self.session.unkeep(self.outputs)

    def fetch_outputs(self):
        """Fetch all outputs of the task and return it as a list"""
        return [output.fetch() for output in self.outputs]

    @property
    def output(self):
        count = len(self.outputs)
        if count == 0 or count > 1:
            raise RainException("Task {!r} has no unique output (outputs={})"
                                .format(self, count))
        return self.outputs[0]

    @property
    def id_pair(self):
        return (self.id, self.session.session_id)

    def to_capnp(self, out):
        out.id.id = self.id
        out.id.sessionId = self.session.session_id
        out.init("inputs", len(self.inputs))

        for i, (key, dataobj) in enumerate(self.inputs.items()):
            out.inputs[i].id.id = dataobj.id
            out.inputs[i].id.sessionId = dataobj.session.session_id
            if key:
                out.inputs[i].label = key

        out.init("outputs", len(self.outputs))
        for i, dataobj in enumerate(self.outputs):
            out.outputs[i].id = dataobj.id
            out.outputs[i].sessionId = dataobj.session.session_id

        out.taskType = self.task_type
        out.taskType = self.task_type
        self.attributes["resources"] = {"cpus": self.resources.n_cpus}
        attributes_to_capnp(self.attributes, out.attributes)

    def wait(self):
        self.session.wait((self,), ())

    def update(self):
        self.session.update((self,))

    def __repr__(self):
        return "<Task id={}/{} type={}>".format(
            self.session.session_id, self.id, self.task_type)

    def __reduce__(self):
        """Speciaization to replace with subworker.unpickle_input_object
        in Python task args while (cloud)pickling. Raises RainError when
        using task with `len(outputs) != 1` as a data object."""
        from . import pycode
        if pycode._global_pickle_inputs is None:
            # call normal __reduce__
            return super().__reduce__()
        return self.output.__reduce__()
