from .session import get_active_session
from .data import DataObject, to_dataobj
from .output import OutputBase
from ..common import RainException, ID, LabeledList
from ..common.attributes import TaskSpec, TaskSpecInput

import traceback


_task_type_register = {}


class TaskMeta(type):
    """A metaclass to register all subclasses of Task in `_task_type_register`."""
    def __new__(meta, name, bases, clsdict):
        global _task_type_register
        cls = type.__new__(meta, name, bases, clsdict)
        task_type = cls.TASK_TYPE
        if task_type is not None:
            _task_type_register[task_type] = cls
        return cls


class Task(metaclass=TaskMeta):
    """
    A task instance in the task graph.

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
        task_type (`str`): Task-type name known to rain governors.
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
        spec (`TaskSpec`): Task specification data.
        info (`TaskInfo`): Task state information on last update.
        state (`TaskState` enum): Task state on last update.
        stack (str): Text description of stack when task was created, used for debug messages
    """
    TASK_TYPE = None
    # State of object
    # None = Not submitted
    _state = None
    _stack = None

    def __init__(self,
                 inputs,
                 outputs, *,
                 config=None,
                 session=None,
                 task_type=None,
                 cpus=1,
                 user_spec=None):
        self._spec = TaskSpec()
        self._info = None

        if session is None:
            session = get_active_session()
        self._session = session
        self._spec.id = session._register_task(self)
        assert isinstance(self.id, ID)

        if task_type is not None:
            self._spec.task_type = task_type
        else:
            if self.TASK_TYPE is None:
                raise ValueError(
                    "Provide {}.TASK_TYPE or task_type=... information."
                    .format(self.__class__))
            self._spec.task_type = self.TASK_TYPE

        if config is not None:
            self._spec.config = config

        if cpus is not None:
            self._spec.resources['cpus'] = cpus

        def to_data_object(o):
            if isinstance(o, str):
                return DataObject(label=o, session=session)
            if isinstance(o, OutputBase):
                return o.create_data_object(session=session)
            if isinstance(o, DataObject):
                return o
            raise TypeError("Only `OutputBase`, `DataObject` and `str` allowed as outputs.")

        if outputs is None:
            outputs = ()
        elif isinstance(outputs, int):
            outputs = tuple(DataObject(session=session) for i in range(outputs))
        else:
            outputs = tuple(to_data_object(obj) for obj in outputs)

        self._outputs = LabeledList(pairs=((output._spec.label, output)
                                           for output in outputs))
        self._spec.outputs = [o.id for o in self._outputs]

        input_pairs = []
        for input in inputs:
            if isinstance(input, tuple):
                label, inp = input
                input_pairs.append((label, to_dataobj(inp)))
            else:
                input_pairs.append((None, to_dataobj(input)))
        self._inputs = LabeledList(pairs=input_pairs)
        self._spec.inputs = [TaskSpecInput(id=i.id, label=lab) for lab, i in self._inputs.items()]

        stack = traceback.extract_stack(None, 6)
        stack.pop()  # Last entry is not usefull, it is actually line above
        self._stack = "".join(traceback.format_list(stack))

    @property
    def id(self):
        """Getter for Task ID."""
        return self._spec.id

    @property
    def state(self):
        """Getter for Task state on last update."""
        return self._state

    @property
    def spec(self):
        """Getter for Task specification. Read only!"""
        return self._spec

    @property
    def info(self):
        """Getter for Task info on last update (`None` when never updated). Read only!"""
        return self._info

    @property
    def task_type(self):
        """Getter for task_type identifier."""
        return self._spec.task_type

    @property
    def inputs(self):
        """Getter for inputs LabeledList. Read only!"""
        return self._inputs

    @property
    def outputs(self):
        """Getter for outputs LabeledList. Read only!"""
        return self._outputs

    @property
    def output(self):
        """Getter for the only output of the task. Fails if `len(self.outputs)!=1`."""
        count = len(self.outputs)
        if count == 0 or count > 1:
            raise RainException("Task {!r} has no unique output (outputs={})"
                                .format(self, count))
        return self.outputs[0]

    def keep_outputs(self):
        """Keep all output objects of the task."""
        for output in self.outputs:
            output.keep()

    def unkeep_outputs(self):
        """Unkeep all output objects of the task."""
        for output in self.outputs:
            output.unkeep()

    def fetch_outputs(self):
        """Fetch all outputs of the task.

        Returns:
            [`DataInstance`]: Fetched output data."""
        return [output.fetch() for output in self.outputs]

    def wait(self):
        """Wait for the task to complete. See `Session.wait()`."""
        self._session.wait((self,))

    def update(self):
        """Update task state and attributes. See `Session.update()`."""
        self._session.update((self,))

    def __repr__(self):
        return "<{} {}, inputs {}, outputs {}>".format(
            self.__class__.__name__, self.id, self.spec.task_type, self.inputs, self.outputs)

    def __reduce__(self):
        """Speciaization to replace with executor.unpickle_input_object
        in Python task args while (cloud)pickling. Raises RainError when
        using task with `len(outputs) != 1` as a data object."""
        from . import pycode
        if pycode._global_pickle_inputs is None:
            # call normal __reduce__
            return super().__reduce__()
        return self.output.__reduce__()
