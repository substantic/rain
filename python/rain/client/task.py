

class Task:

    # State of object
    # None = Not submitted
    state = None

    def __init__(self, session):
        self.session = session
        session.register_task(self)