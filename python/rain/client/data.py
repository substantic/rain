
import capnp

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

    # Value of data object (value can be filled by client if it is constant,
    # or by fetching from server)
    data = None

    type = None
    # Type of object, this should be set by subclass

    def __init__(self, label=None, session=None):
        if session is None:
            session = get_active_session()
        self.session = session
        self.label = label
        self.id = session.register_dataobj(self)

    @property
    def id_pair(self):
        return (self.id, self.session.session_id)

    def _free(self):
        """Set flag that object is not available on the server """
        self._keep = False

    def unkeep(self):
        """Remove data object from the server"""
        self.session.unkeep((self,))

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
        out.id.sessionId = self.session.session_id
        out.keep = self._keep
        if self.label:
            out.label = self.label
        out.type = common.DataObjectType.blob

        if self.data is not None:
            out.hasData = True
            out.data = self.data

    def wait(self):
        self.session.wait((), (self,))

    def fetch(self):
        return self.session.fetch(self)

    def update(self):
        self.session.update((self,))

    def __del__(self):
        if self.state is not None and self._keep:
            try:
                self.session.client._unkeep((self,))
            except capnp.lib.capnp.KjException:
                # Ignore capnp exception, since this constructor may be
                # called when connection is closed
                pass

    def is_blob(self):
        return self.type == common.DataObjectType.blob

    def is_directory(self):
        return self.type == common.DataObjectType.directory


class Blob(DataObject):

    type = common.DataObjectType.blob

    def __repr__(self):
        return "<Blob {} {}/{}>".format(
            self.label, self.session.session_id, self.id)


class Directory(DataObject):

    type = common.DataObjectType.directory

    def get_blob(self, path):
        return DataObjectPart(self, path, common.DataObjectType.blob)

    def get_directory(self, path):
        return DataObjectPart(self, path, common.DataObjectType.directory)

    def fetch_listing(self):
        """Returns a list of nodes in directory"""
        raise Exception("Not implemented")

    def __repr__(self):
        return "<Directory {}/{}>".format(self.session.session_id, self.id)


class DataObjectPart:

    def __init__(self, dataobject, path, type):
        self.dataobject = dataobject
        self.path = path
        self.type = type

    def make_dataobject(self):
        """Return DataObject created from DataObject part"""
        raise Exception("TODO")

    def fetch(self):
        raise Exception("TODO")


def blob(value, label=""):
    """Create a constant data object"""

    if isinstance(value, str):
        value = bytes(value, "utf-8")
    elif not isinstance(value, bytes):
        raise RainException(
            "Invalid blob type (only str or bytes are allowed)")

    dataobj = Blob(label)
    dataobj.data = value
    dataobj.label = "const"
    return dataobj


def to_data(obj):
    """Convert an object to DataObject/DataObjectPart"""
    if isinstance(obj, DataObject):
        return obj
    if isinstance(obj, DataObjectPart):
        return obj
    if isinstance(obj, Task):
        if len(obj.outputs) == 1:
            return obj.outputs[0]
        if len(obj.outputs) == 0:
            raise RainException("{} does not have any output".format(obj))
        else:
            raise RainException("{} returns more outputs".format(obj))

    if isinstance(obj, str) or isinstance(obj, bytes):
        return blob(obj)

    raise RainException("{!r} cannot be used as data object".format(obj))


from .task import Task  # noqa
