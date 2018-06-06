
"""
Global stack of active sessions
Do not directly acces to this array
but use

>>> with session:
...     pass

or function

get_active_session()
"""

from rain.client import rpc
from ..common import RainException, ID
from . import graph

_global_sessions = []

# TODO: Check attribute "active" before making remote calls


def global_session_push(session):
    global _global_sessions
    if not session.active:
        raise RainException("Session is closed")
    _global_sessions.append(session)


def global_session_pop():
    global _global_sessions
    return _global_sessions.pop()


class SessionBinder:
    """This class is returned when session.bind_only() is used"""

    def __init__(self, session):
        self.session = session

    def __enter__(self):
        global_session_push(self.session)
        return self.session

    def __exit__(self, type, value, traceback):
        s = global_session_pop()
        assert self.session is s


class Session:
    """
    A container for one task graph.

    Do not create directly, rather using :func:`Client.new_session`.
    When used as a context manager, all new objects and tasks are created
    within the session. Note the session is closed afterwards.

    >>> with client.new_session() as s:
    ...     bl = blob("Hello rain!")
    ...     tsk = tasks.sleep(1.0, bl)
    ...     tsk.output.keep()
    ...     s.submit()
    ...     print(tsk.output.fetch()) # waits for completion

    Currently, the graph and objects are alive on the server only as long as
    the `Session` exists.
    """

    def __init__(self, client, session_id):
        self.active = True  # True if a session is live in server
        self.client = client
        self.session_id = session_id

        self._tasks = []  # Unsubmitted task
        self._dataobjs = []  # Unsubmitted objects
        self._id_counter = 9
        self._submitted_tasks = []
        self._submitted_dataobjs = []

        # Cache for not submited constants: bytes/str -> DataObject
        # It is cleared on submit
        # TODO: It is not now implemented
        self._const_cache = {}

        # Static data serves for internal usage of client.
        # It is not directly available to user
        # It is used to store e.g. for serialized Python objects
        self._static_data = {}

    @property
    def task_count(self):
        """The number of unsubmitted tasks."""
        return len(self._tasks)

    @property
    def dataobj_count(self):
        """The number of unsubmitted objects."""
        return len(self._dataobjs)

    def __enter__(self):
        global_session_push(self)
        return self

    def __exit__(self, type, value, traceback):
        s = global_session_pop()
        assert s is self
        self.close()

    def __repr__(self):
        return "<Session session_id={}>".format(self.session_id)

    def close(self):
        """Closes session; all tasks are stopped, all objects freed."""
        if self.active and self.client:
            self.client._close_session(self)
        self._tasks = []
        self._dataobjs = []
        self._submitted_dataobjs = []
        self._submitted_dataobjs = []
        self.active = False

    def bind_only(self):
        """
        This method serves to bind session without autoclose functionality.

        >>> with session.bind_only() as s:
        ...     doSometing()

        binds the session, but do not close it at the end (so it may be bound
        again either with `bind_only` or normally with `with session: ...`).
        """
        return SessionBinder(self)

    def _register_task(self, task):
        """Register task into session.

        Returns:
            ID: the assigned id."""
        assert task._session == self and task.id is None
        self._tasks.append(task)
        self._id_counter += 1
        return ID(session_id=self.session_id, id=self._id_counter)

    def _register_dataobj(self, dataobj):
        """Register data object into session.

        Returns:
            ID: the assigned id."""
        assert dataobj._session == self and dataobj.id is None
        self._dataobjs.append(dataobj)
        self._id_counter += 1
        return ID(session_id=self.session_id, id=self._id_counter)

    def keep_all(self):
        """Set keep flag for all unsubmitted objects"""
        for dataobj in self._dataobjs:
            dataobj.keep()

    def submit(self):
        """"Submit all unsubmitted objects."""
        self.client._submit(self._tasks, self._dataobjs)
        for task in self._tasks:
            task._state = rpc.common.TaskState.notAssigned
            self._submitted_tasks.append(task)
        for dataobj in self._dataobjs:
            dataobj._state = rpc.common.DataObjectState.unfinished
            self._submitted_dataobjs.append(dataobj)
        self._tasks = []
        self._dataobjs = []

    def _split_tasks_objects(self, items):
        """Split `items` into `Task`s and `DataObject`s, raisong error on anything else.

        Returns:
            `(tasks, dataobjs)`"""
        from . import Task, DataObject
        tasks, dataobjs = [], []
        for i in items:
            if isinstance(i, Task):
                tasks.append(i)
            elif isinstance(i, DataObject):
                dataobjs.append(i)
            else:
                raise TypeError("Neither Task or DataObject: {!r}".format(i))
        return (tasks, dataobjs)

    def wait(self, items):
        """Wait until *all* specified tasks and dataobjects are finished."""
        tasks, dataobjs = self._split_tasks_objects(items)
        self.client._wait(tasks, dataobjs)

        for task in tasks:
            task._state = rpc.common.TaskState.finished

        for dataobj in dataobjs:
            dataobj._state = rpc.common.DataObjectState.finished

    def wait_some(self, items):
        """Wait until *some* of specified tasks/dataobjects are finished.

        Returns:
            `(finished_tasks, finished_dataobjs)`"""
        tasks, dataobjs = self._split_tasks_objects(items)
        finished_tasks, finished_dataobjs = self.client._wait_some(
            tasks, dataobjs)

        for task in finished_tasks:
            task._state = rpc.common.TaskState.finished

        for dataobj in finished_dataobjs:
            dataobj._state = rpc.common.DataObjectState.finished

        return finished_tasks, finished_dataobjs

    def wait_all(self):
        """Wait until all submitted tasks and objects are finished."""
        self.client._wait_all(self)

        for task in self._submitted_tasks:
            task._state = rpc.common.TaskState.finished

        for dataobj in self._submitted_dataobjs:
            dataobj._state = rpc.common.DataObjectState.finished

    def fetch(self, dataobject):
        """Wait for the object to finish, update its state and
        fetch the object data.

        Returns:
            `DataInstance`: The object data proxy."""
        return self.client._fetch(dataobject)

    def unkeep(self, dataobjects):
        """Unset keep flag for given objects."""
        submitted = []
        from . import DataObject
        for dataobj in dataobjects:
            if not isinstance(dataobj, DataObject):
                raise TypeError("Not a DataObject: {!r}".format(dataobj))
            if not dataobj.is_kept():
                raise RainException("Object {} is not kept".format(dataobj.id))
            if dataobj.state is not None:
                submitted.append(dataobj)
            else:
                dataobj._keep = False

        if not submitted:
            return

        self.client._unkeep(submitted)

        for dataobj in submitted:
            dataobj._free()

    def update(self, items):
        """Update the status and metadata of given tasks and objects."""
        self.client.update(items)

    def make_graph(self, show_ids=True):
        """Create a graph of tasks and objects that were *not yet* submitted."""

        def add_obj(o):
            if o is None:
                return
            node = g.node(o)
            node.label = o.id
            node.shape = "box"
            node.color = "none"
            node.fillcolor = "#0088aa"
            node.fillcolor = "#44ccff"
            if o.is_kept():
                node.fillcolor = "#44ccff"
                node.color = "black"

        def add_task(t):
            if t is None:
                return
            node = g.node(t)
            node.label = "{}\n{}".format(t.id_pair, t.task_type)
            node.shape = "oval"
            node.fillcolor = "#0088aa"
            node.color = "none"
            for i, (key, o) in enumerate(t.inputs.items()):
                if key is None:
                    label = str(i)
                else:
                    label = "{}: {}".format(i, key)
                g.node(o).add_arc(node, label)

            for i, (key, o) in enumerate(t.outputs.items()):
                if key is None:
                    label = str(i)
                else:
                    label = "{}: {}".format(i, key)
                node.add_arc(g.node(o), label)

        g = graph.Graph()

        for o in self._dataobjs:
            add_obj(o)

        for o in self._submitted_dataobjs:
            add_obj(o)

        for t in self._tasks:
            add_task(t)

        for t in self._submitted_tasks:
            add_task(t)

        return g


def get_active_session():
    """Internal helper to get innermost active `Session`."""
    if not _global_sessions:
        raise RainException("No active session")
    else:
        return _global_sessions[-1]
