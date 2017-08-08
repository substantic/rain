
from .session import get_active_session
from .common import RainException


class DataObject:

    id = None

    # Flag if data object should be kept on server
    _keep = False

    # State of object
    # None = Not submitted
    state = None

    # Value of data object (value can be filled by client if it is constant, or by fetching from server)
    value = None

    def __init__(self, session):
        self.session = session
        self.id = session.register_dataobj(self)

    def free(self):
        """Free data object from server. Throws an error if self.keep is False or not submitted """
        self.session.free(self)

    def keep(self):
        """Set flag that is object should be kept on the server"""
        self._keep = True

    def __del__(self):
        if self.state is not None and self._keep:
            self.session.free(self)


def blob(value):
    """Create a constant data object"""

    if isinstance(value, str):
        value = bytes(value, "utf-8")
    elif not isinstance(value, bytes):
        raise RainException("Invalid blob type (only str or bytes are allowed)")

    dataobj = DataObject(get_active_session())
    dataobj.value = value
    return dataobj