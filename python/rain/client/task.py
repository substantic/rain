
from .session import get_active_session
from .data import Blob, to_data
from .common import RainException
from .table import Table
from ..common.resources import cpu_1


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
        self.id = session.register_task(self)

        self.task_type = task_type
        self.task_config = task_config
        self.additionals = None

        if outputs is None:
            outputs = ()
        elif isinstance(outputs, int):
            outputs = tuple(Blob(session=session) for i in range(outputs))
        else:
            outputs = tuple(Blob(obj) if isinstance(obj, str) else obj
                            for obj in outputs)
        self.outputs = Table(outputs, {output.label: output
                                       for output in outputs if output.label})

        input_objects = []
        input_labels = {}

        for input in inputs:
            if isinstance(input, tuple):
                assert len(input) == 2
                input_objects.append(to_data(input[1]))
                input_labels[input[0]] = input[1]
            else:
                input_objects.append(to_data(input))

        self.inputs = Table(input_objects, input_labels)

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

    def get(self, name):
        try:
            if self.additionals:
                return self.additionals[name]
        except KeyError:
            pass
        raise RainException("Additional '{}' not found".format(name))

    def to_capnp(self, out):
        out.id.id = self.id
        out.id.sessionId = self.session.session_id
        out.init("inputs", len(self.inputs))

        i = 0
        for key, dataobj in self.inputs.label_pairs():
            out.inputs[i].id.id = dataobj.id
            out.inputs[i].id.sessionId = dataobj.session.session_id
            if key:
                out.inputs[i].label = key
            i += 1

        out.init("outputs", len(self.outputs))
        i = 0
        for dataobj in self.outputs:
            out.outputs[i].id = dataobj.id
            out.outputs[i].sessionId = dataobj.session.session_id
            i += 1

        out.taskType = self.task_type

        if self.task_config:  # We need this since, task_config may be None
            out.taskConfig = self.task_config

        self.resources.to_capnp(out.resources)
        out.taskType = self.task_type

    def wait(self):
        self.session.wait((self,), ())

    def update(self):
        self.session.update((self,))

    def __repr__(self):
        return "<Task id={}/{} type={}>".format(
            self.session.session_id, self.id, self.task_type)
