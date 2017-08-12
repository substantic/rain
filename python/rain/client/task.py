
from .session import get_active_session
from .data import DataObject, to_data
from .common import RainException


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
                 outputs=("",),
                 session=None):
        if session is None:
            session = get_active_session()
        self.session = session
        self.id = session.register_task(self)

        self.task_type = task_type
        self.task_config = task_config

        self.outputs = {}
        self.inputs = tuple(to_data(obj) for obj in inputs)

        for output in outputs:
            self._add_output(output)

    def _add_output(self, name):
        """Create a new output returned by this task.
           It should be called only during task creation.

           If 'name' is identifier, data object is also put into
           out_<name> attribute.
        """
        if isinstance(name, str):
            dataobj = DataObject(self.session)
            self.outputs[name] = dataobj
            if name.isidentifier():
                setattr(self, "out_" + name, dataobj)
            elif name == "":
                setattr(self, "out", dataobj)
        else:
            raise Exception(
                "'{}' is not valid output name".format(repr(name)))

    def has_output(self, name):
        return name in self.outputs

    def to_capnp(self, out):
        out.id.id = self.id
        out.id.sessionId = self.session.session_id
        out.init("inputs", len(self.inputs))
        for tii in range(len(self.inputs)):
            out.inputs[tii].id.id = self.inputs[tii].id
            out.inputs[tii].id.sessionId = self.inputs[tii].session.session_id
            out.inputs[tii].label = ""

        out.init("outputs", len(self.outputs))
        toi = 0
        for out_label, out_task in self.outputs.items():
            out.outputs[toi].id.id = out_task.id
            out.outputs[toi].label = out_label
            toi += 1
        out.taskType = self.task_type

        if self.task_config:  # We need this since, task_config may be None
            out.taskConfig = self.task_config

        out.nCpus = self.n_cpus
        out.taskType = self.task_type

    def __getitem__(self, name):
        output = self.outputs.get(name)
        if output is None:
            raise RainException("Task {!r} has no output {!r}", self, name)
        return output

    def __repr__(self):
        return "<Task id={}/{} type={}>".format(self.session.session_id, self.id, self.task_type)

