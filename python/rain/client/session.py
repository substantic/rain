
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

from .common import RainException

class Session:

    def __init__(self, client, session_id):
        self.client = client
        self.session_id = session_id
        self.tasks = []
        self.dataobjs = []
        self.id_counter = 9

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

    def free(self, dataobj):
        """Free data object from server. Throws an error if self.keep is False or not submitted """
        if dataobj.state is None:
            raise RainException("Object is not submitted")
        if not dataobj._keep:
            raise RainException("Object is not kept on server")
        dataobj._keep = False
        raise Exception("Not implemented")

    def submit(self):
        """"Submit all registered objects"""
        self.client._submit(self.tasks, self.dataobjs)
        self.tasks = []
        self.dataobjs = []

    def wait_all(self):
        """Wait until all registered tasks are finished"""
        pass


def get_active_session():
    if not _global_sessions:
        raise RainException("No active session")
    else:
        return _global_sessions[-1]
