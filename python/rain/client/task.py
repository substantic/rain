
from .session import get_active_session
from .data import DataObject, to_data
from .common import RainException
from .table import Table


class Task:
    # State of object
    # None = Not submitted
    state = None
    id = None
    n_cpus = 1

    def __init__(self,
                 task_type,
                 task_config=None,
                 inputs=(),
                 outputs=("output",),
                 session=None):
        if session is None:
            session = get_active_session()
        self.session = session
        self.id = session.register_task(self)

        self.task_type = task_type
        self.task_config = task_config

        self.out = Table({name: DataObject(self.session) for name in outputs})

        if isinstance(inputs, tuple):
            self.inputs = Table(tuple(to_data(obj) for obj in inputs))
        else:
            self.inputs = Table({name: to_data(obj) for name, obj in inputs.items()})

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
            out.outputs[i].id.id = dataobj.id
            out.outputs[i].id.sessionId = dataobj.session.session_id
            out.outputs[i].label = key
            i += 1

        out.taskType = self.task_type

        if self.task_config:  # We need this since, task_config may be None
            out.taskConfig = self.task_config

        out.nCpus = self.n_cpus
        out.taskType = self.task_type

    def wait(self):
        self.session.wait((self,), ())

    def __repr__(self):
        return "<Task id={}/{} type={}>".format(self.session.session_id, self.id, self.task_type)

