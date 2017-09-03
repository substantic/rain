
"""
Global stack of active sessions
Do not directly acces to this array
but use

>>> with session:
...     pass

or function

get_active_session()
"""
_global_sessions = []

import weakref

from rain.client import rpc

from .common import RainException

class Session:

    def __init__(self, client, session_id):
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

    @property
    def task_count(self):
        return len(self.tasks)

    @property
    def dataobj_count(self):
        return len(self.dataobjs)

    def __enter__(self):
        global _global_sessions
        _global_sessions.append(self)
        return self

    def __exit__(self, type, value, traceback):
        s = _global_sessions.pop()
        assert s == self

    def __repr__(self):
        return "<Session session_id={}>".format(self.session_id)

    def register_task(self, task):
        """Register task into session; returns assigned id"""
        assert task.session == self and task.id is None
        self.tasks.append(task)
        self.id_counter += 1
        return self.id_counter

    def register_dataobj(self, dataobj):
        """Register data object into session; returns assigned id"""
        assert dataobj.session == self  and dataobj.id is None
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
            dataobj.state = rpc.common.DataObjectState.notAssigned
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

    def remove(self, dataobjects):
        """Remove data objects"""
        for dataobj in dataobjects:
            if not dataobj.is_kept():
                raise RainException("Object {} is not kept".format(dataobj.id))
            if dataobj.state is None:
                raise RainException("Object {} not submitted".format(dataobj.id))
            if dataobj.state == rpc.common.DataObjectState.removed:
                raise RainException("Object {} already removed".format(dataobj.id))

        self.client._unkeep(dataobjects)

        for dataobj in dataobjects:
            dataobj._free()

    def get_state(self, tasks, dataobjects):
        self.client._get_state(tasks, dataobjects)


def get_active_session():
    if not _global_sessions:
        raise RainException("No active session")
    else:
        return _global_sessions[-1]
