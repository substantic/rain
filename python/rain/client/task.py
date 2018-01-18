from .session import get_active_session
from .data import DataObject, to_data
from .output import Output
from ..common import RainException, ID, LabeledList, ids
from ..common.resources import cpu_1
from ..common.attributes import attributes_to_capnp


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
        assert isinstance(self.id, ID)

        self.task_type = task_type
        self.attributes = {}

        if config is not None:
            self.attributes["config"] = config

        def to_data_object(o):
            if isinstance(o, int):
                o = "out{}".format(o)
            if isinstance(o, str):
                return DataObject(label=o, session=session)
            if isinstance(o, Output):
                return o.create_data_object(session=session)
            if isinstance(o, DataObject):
                return o
            raise TypeError("Anly `Output` and `str` allowed as outputs.")

        if outputs is None:
            outputs = ()
        elif isinstance(outputs, int):
            outputs = tuple(to_data_object(i) for i in range(outputs))
        else:
            outputs = tuple(to_data_object(obj) for obj in outputs)

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

    def to_capnp(self, out):
        ids.id_to_capnp(self.id, out.id)
        out.init("inputs", len(self.inputs))

        for i, (key, dataobj) in enumerate(self.inputs.items()):
            ids.id_to_capnp(dataobj.id, out.inputs[i].id)
            if key:
                out.inputs[i].label = key

        out.init("outputs", len(self.outputs))
        for i, dataobj in enumerate(self.outputs):
            ids.id_to_capnp(dataobj.id, out.outputs[i])

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
