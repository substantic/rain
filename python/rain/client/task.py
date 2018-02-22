from .session import get_active_session
from .data import DataObject, to_data
from .output import Output
from ..common import RainException, ID, LabeledList, ids
from ..common.attributes import attributes_to_capnp


class Task:
    """
    A single task instance in the task graph.

    `__init__` creates a single task instance, inserts it into `Session`
    and assigns it an `ID`. Creates output `DataObject` instances based
    on `outputs` given.

    `Task` is commonly created by functions in `rain.client.tasks`, or task builders
    created by `Remote` or `Program`. Always belongs to a `Session` and has a valid `ID`.
    You may wish to call it explicitely (or subclass it) when creating your own task-types.

    Particular task types are not realized via subclasses but
    with string `task_type` attribute. (Subclassing may be introduced later.)

    The task state is *not* automatically updated by the server. The state and
    attributes are updated on `Task.update()`, `Task.fetch()` and `Task.wait()`.

    Args:
        task_type (`str`): Task-type name known to rain workers.
        config: Any task-specific config.
        inputs (`LabeledList` or sequence): Sequence of `Input` or `DataObject`.
        outputs (`LabeledList` or sequence): Specification of `Output`\ s for the task.
        session (`Session` or `None`): Session to create the task in.
            If not specified, the current `Session` is used.
        cpus (`int`): Number of cpus.

    Attributes:
        id (`ID`): Auto-assigned task ID.
        inputs (`LabeledList[DataObject]`): Input objects.
        outputs (`LabeledList[DataObject]`): Output objects created by the task.
        output (`DataObject`): Shortcut for `outputs[0]`. Raises Exception on multiple outputs.
        attributes (`dict`): Task attributes. See attributes_ for details.
        state (`TaskState` enum): Task state on last update.
    """
    # State of object
    # None = Not submitted
    state = None
    id = None
    config = None

    def __init__(self,
                 task_type,
                 config=None,
                 inputs=(),
                 outputs=None,
                 session=None,
                 cpus=1):
        if session is None:
            session = get_active_session()
        self.session = session
        self.id = session._register_task(self)
        assert isinstance(self.id, ID)

        self.task_type = task_type
        self.attributes = {}

        if config is not None:
            self.attributes["config"] = config

        if cpus is not None:
            self.attributes["resources"] = {"cpus": cpus}

        def to_data_object(o):
            if isinstance(o, int):
                o = "out{}".format(o)
            if isinstance(o, str):
                return DataObject(label=o, session=session)
            if isinstance(o, Output):
                return o.create_data_object(session=session)
            if isinstance(o, DataObject):
                return o
            raise TypeError("Only `Output` and `str` allowed as outputs.")

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
        """Keep all output objects of the task."""
        for output in self.outputs:
            output.keep()

    def unkeep_outputs(self):
        """Unkeep all output objects of the task."""
        self.session.unkeep(self.outputs)

    def fetch_outputs(self):
        """Fetch all outputs of the task.

        Returns:
            [`DataInstance`]: Fetched output data."""
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
        attributes_to_capnp(self.attributes, out.attributes)

    def wait(self):
        """Wait for the task to complete. See `Session.wait()`."""
        self.session.wait((self,))

    def update(self):
        """Update task state and attributes. See `Session.update()`."""
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
