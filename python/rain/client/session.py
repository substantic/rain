
"""
Global stack of active sessions
Do not directly acces to this array
but use

>>> with session:
...     pass

or function

get_active_session()
"""

import weakref

from rain.client import rpc
from .common import RainException

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

    def __init__(self, client, session_id):
        self.active = True
        self.client = client
        self.session_id = session_id
        self.tasks = []
        self.dataobjs = []
        self.id_counter = 9
        self.submitted_tasks = []
        self.submitted_dataobjs = []

        # Cache for not submited constants: bytes/str -> DataObject
        # It is cleared on submit
        # TODO: It is not now implemented
        self.const_cache = {}

        # Static data serves for internal usage of client.
        # It is not directly available to user
        # It is used to store e.g. for serialized Python objects
        self.static_data = {}

    @property
    def task_count(self):
        return len(self.tasks)

    @property
    def dataobj_count(self):
        return len(self.dataobjs)

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
        if self.active and self.client:
            self.client._close_session(self)
        self.active = False

    def bind_only(self):
        """
        This method serves to bind session without autoclose functionality

        >>> with session.bind_only() as s:
        ...     doSometing()

        binds the session, but do not close it at the end. Except closing it the same as

        >>> with session as s:
        ...     doSometing()
        """
        return SessionBinder(self)

    def register_task(self, task):
        """Register task into session; returns assigned id"""
        assert task.session == self and task.id is None
        self.tasks.append(task)
        self.id_counter += 1
        return self.id_counter

    def register_dataobj(self, dataobj):
        """Register data object into session; returns assigned id"""
        assert dataobj.session == self and dataobj.id is None
        self.dataobjs.append(dataobj)
        self.id_counter += 1
        return self.id_counter

    def submit(self):
        """"Submit all registered objects"""
        self.client._submit(self.tasks, self.dataobjs)
        for task in self.tasks:
            task.state = rpc.common.TaskState.notAssigned
            self.submitted_tasks.append(weakref.ref(task))
        for dataobj in self.dataobjs:
            dataobj.state = rpc.common.DataObjectState.unfinished
            self.submitted_dataobjs.append(weakref.ref(dataobj))
        self.tasks = []
        self.dataobjs = []

    def wait(self, tasks, dataobjs):
        """Wait until specified tasks/dataobjects are finished"""
        self.client._wait(tasks, dataobjs)

        for task in tasks:
            task.state = rpc.common.TaskState.finished

        for dataobj in dataobjs:
            dataobj.state = rpc.common.DataObjectState.finished

    def wait_some(self, tasks, dataobjects):
        """Wait until some of specified tasks/dataobjects are finished"""
        finished_tasks, finished_dataobjs = self.client._wait_some(tasks, dataobjects)

        for task in finished_tasks:
            task.state = rpc.common.TaskState.finished

        for dataobj in finished_dataobjs:
            dataobj.state = rpc.common.DataObjectState.finished

        return finished_tasks, finished_dataobjs

    def wait_all(self):
        """Wait until all registered tasks are finished"""
        self.client._wait_all(self.session_id)

        for task in self.submitted_tasks:
            if task:
                task().state = rpc.common.TaskState.finished

        for dataobj in self.submitted_dataobjs:
            if dataobj:
                dataobj().state = rpc.common.DataObjectState.finished

    def fetch(self, dataobject):
        return self.client._fetch(dataobject)

    def unkeep(self, dataobjects):
        """Remove data objects"""
        for dataobj in dataobjects:
            if not dataobj.is_kept():
                raise RainException("Object {} is not kept".format(dataobj.id))
            if dataobj.state is None:
                raise RainException("Object {} not submitted".format(dataobj.id))

        self.client._unkeep(dataobjects)

        for dataobj in dataobjects:
            dataobj._free()

    def update(self, items):
        self.client.update(items)


def get_active_session():
    if not _global_sessions:
        raise RainException("No active session")
    else:
        return _global_sessions[-1]
