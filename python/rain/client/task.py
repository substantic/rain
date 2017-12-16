from .session import get_active_session
from .data import Blob, to_data
from .common import RainException
from ..common import LabeledList
from ..common.resources import cpu_1
from ..common.attributes import attributes_to_capnp


class Task:

    # State of object
    # None = Not submitted
    state = None
    id = None
    resources = cpu_1

    def __init__(self,
                 task_type,
                 task_config=None,
                 inputs=(),
                 outputs=None,
                 session=None):
        if session is None:
            session = get_active_session()
        self.session = session
        self.id = session._register_task(self)

        self.task_type = task_type
        self.task_config = task_config
        self.attributes = {}

        if outputs is None:
            outputs = ()
        elif isinstance(outputs, int):
            outputs = tuple(Blob(session=session) for i in range(outputs))
        else:
            outputs = tuple(Blob(obj, session=session) if isinstance(obj, str)
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

        if self.task_config:  # We need this since, task_config may be None
            out.taskConfig = self.task_config

        self.resources.to_capnp(out.resources)
        out.taskType = self.task_type
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
