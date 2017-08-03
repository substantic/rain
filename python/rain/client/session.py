
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


class Session:

    def __init__(self, client, session_id):
        self.client = client
        self.session_id = session_id
        self.tasks = []

    def __enter__(self):
        global _global_sessions
        _global_sessions.append(self)
        return self

    def __exit__(self, type, value, traceback):
        s = _global_sessions.pop()
        assert s == self

    def add_task(self, task):
        assert task.session is None
        task.session = self
        return self.tasks.append(task)

    def submit(self):
        ""'Submit all registered objects'
        pass

    def wait_all(self):
        """Wait until all registered tasks are finished"""
        pass


def get_active_session():
    if not _global_sessions:
        raise Exception("No active session")
    else:
        return _global_sessions[-1]
