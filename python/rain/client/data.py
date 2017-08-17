
from .session import get_active_session
from .common import RainException
from .rpc import common


class DataObject:

    id = None

    # Flag if data object should be kept on server
    _keep = False

    # State of object
    # None = Not submitted
    state = None

    # Value of data object (value can be filled by client if it is constant, or by fetching from server)
    data = None

    def __init__(self, session):
        self.session = session
        self.id = session.register_dataobj(self)

    def _free(self):
        """Set flag that object is not available on the server """
        self._keep = False

    def remove(self):
        """Remove data object from the server"""
        self.session.remove((self,))

    def keep(self):
        """Set flag that is object should be kept on the server"""
        if self.state is not None:
            raise RainException("Cannot keep submitted task")
        self._keep = True

    def is_kept(self):
        """Returns the value of self._keep"""
        return self._keep

    def to_capnp(self, out):
        out.id.id = self.id
        out.keep = self._keep
        if self.data:
            out.data = self.data

    def __del__(self):
        if self.state is not None and self._keep:
            self.session.free(self)

    def __repr__(self):
        return "<DataObject {}/{}>".format(self.session.session_id, self.id)


def blob(value):
    """Create a constant data object"""

    if isinstance(value, str):
        value = bytes(value, "utf-8")
    elif not isinstance(value, bytes):
        raise RainException("Invalid blob type (only str or bytes are allowed)")

    dataobj = DataObject(get_active_session())
    dataobj.data = value
    return dataobj


def to_data(obj):
    """Convert an object to DataObject"""
    if isinstance(obj, DataObject):
        return obj
    if isinstance(obj, Task):
        outputs = obj.outputs.values()
        if len(outputs) == 1:
            return tuple(outputs)[0]
        if len(outputs) == 0:
            raise RainException("{} does not have any output".format(obj))
        else:
            raise RainException("{} returns more outputs".format(obj))

    if isinstance(obj, str) or isinstance(obj, bytes):
        return blob(obj)

    raise RainException("{!r} cannot be used as data object".format(obj))


from .task import Task  # noqa