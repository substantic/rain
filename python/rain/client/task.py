
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
            self.out = Table({"output": Blob("output", session)})
        else:
            out = {}
            for obj in outputs:
                if isinstance(obj, str):
                    do = Blob(obj, session)
                else:
                    do = obj
                out[do.label] = do
            self.out = Table(out)

        if isinstance(inputs, tuple):
            self.inputs = Table(tuple(to_data(obj) for obj in inputs))
        else:
            self.inputs = Table({name: to_data(obj)
                                 for name, obj in inputs.items()})

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

    def has_output(self, name):
        return name in self.out

    def to_capnp(self, out):
        out.id.id = self.id
        out.id.sessionId = self.session.session_id
        out.init("inputs", len(self.inputs))

        i = 0
        for key, dataobj in self.inputs:
            out.inputs[i].id.id = dataobj.id
            out.inputs[i].id.sessionId = dataobj.session.session_id
            out.inputs[i].label = str(key)
            i += 1

        out.init("outputs", len(self.out))
        i = 0
        for key, dataobj in self.out:
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
