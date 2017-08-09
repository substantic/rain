
from .session import get_active_session
from .common import RainException


class DataObject:

    id = None

    # Flag if data object should be kept on server
    _keep = False

    # State of object
    # None = Not submitted
    state = None

    # Value of dataca object (value can be filled by client if it is constant, or by fetching from server)
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

    def __repr__(self):
        return "<DataObject {}/{}>".format(self.session.session_id, self.id)


def blob(value):
    """Create a constant data object"""

    if isinstance(value, str):
        value = bytes(value, "utf-8")
    elif not isinstance(value, bytes):
        raise RainException("Invalid blob type (only str or bytes are allowed)")

    dataobj = DataObject(get_active_session())
    dataobj.value = value
    return dataobj


def to_data(obj):
    """Convert an object to DataObject"""
    if isinstance(obj, DataObject):
        return obj
    if isinstance(obj, Task):
        outputs = obj.outputs.values()
        if len(outputs) != 1:
            return outputs[0]
        if len(outputs) == 0:
            raise RainException("{} does not have any output".format(obj))
        else:
            raise RainException("{} returns more outputs".format(obj))

    if isinstance(obj, str) or isinstance(obj, bytes):
        # TODO: Check cache / call blob
        raise Exception("Not implemented")

    raise RainException("{!r} cannot be used as data object".format(obj))


from .task import Task  # noqa
