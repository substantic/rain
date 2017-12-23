import capnp

from .session import get_active_session
from .common import RainException
from ..common.attributes import attributes_to_capnp


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

    def __init__(self, label=None, session=None, content_type=None):
        if session is None:
            session = get_active_session()
        self.session = session
        self.label = label
        self.id = session._register_dataobj(self)
        self.content_type = content_type
        self.attributes = {}

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

        if self.data is not None:
            out.hasData = True
            out.data = self.data

        attributes_to_capnp(self.attributes, out.attributes)

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
        return self.content_type != "dir"

    def is_directory(self):
        return self.content_type == "dir"

    def __reduce__(self):
        """Speciaization to replace with subworker.unpickle_input_object
        in Python task args while (cloud)pickling."""
        from . import pycode
        from ..subworker import subworker
        if pycode._global_pickle_inputs is None:
            # call normal __reduce__
            return super().__reduce__()
        base_name, counter, inputs = pycode._global_pickle_inputs
        input_name = "{}{{{}}}".format(base_name, counter)
        pycode._global_pickle_inputs[1] += 1
        inputs.append((input_name, self))
        return (subworker.unpickle_input_object,
                (input_name, len(inputs) - 1, ))

    def __repr__(self):
        return "<Do {} {}/{}>".format(
            self.label, self.session.session_id, self.id)


def blob(value, label="const", content_type=None):
    """Create a constant data object"""

    if isinstance(value, str):
        value = bytes(value, "utf-8")
    elif not isinstance(value, bytes):
        raise RainException(
            "Invalid blob type (only str or bytes are allowed)")

    dataobj = DataObject(label, content_type=content_type)
    dataobj.data = value
    return dataobj


def to_data(obj):
    """Convert an object to DataObject/DataObjectPart"""
    if isinstance(obj, DataObject):
        return obj
    if isinstance(obj, Task):
        if len(obj.outputs) == 1:
            return obj.outputs[0]
        if len(obj.outputs) == 0:
            raise RainException("{} does not have any output".format(obj))
        else:
            raise RainException("{} returns more outputs".format(obj))

    if isinstance(obj, str) or isinstance(obj, bytes):
        raise RainException(
            "Instance of {} cannot be used as an data object\n"
            "Help: You can wrap it by 'blob' to use it as data object"
            .format(type(obj)))

    raise RainException("Instance of {!r} cannot be used as data object"
                        .format(type(obj)))


from .task import Task  # noqa
