import capnp
import tarfile
import io

from .session import get_active_session
from ..common import RainException, ids, ID
from ..common.attributes import attributes_to_capnp
from ..common.content_type import check_content_type, encode_value
from ..common import DataType


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

    def __init__(self, label=None, session=None, data_type=DataType.BLOB, content_type=None):
        assert isinstance(data_type, DataType)
        if session is None:
            session = get_active_session()
        self.session = session
        self.label = label
        self.id = session._register_dataobj(self)
        assert isinstance(self.id, ID)
        self.attributes = {
            "spec": {"content_type": content_type}
        }
        self.data_type = data_type

    @property
    def content_type(self):
        return self.attributes["spec"]["content_type"]

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
        ids.id_to_capnp(self.id, out.id)
        out.keep = self._keep
        if self.label:
            out.label = self.label

        out.dataType = self.data_type.to_capnp()
        if self.data is not None:
            out.data = self.data
            out.hasData = True
        else:
            out.hasData = False
        attributes_to_capnp(self.attributes, out.attributes)

    def wait(self):
        self.session.wait((self,))

    def fetch(self):
        """
        Fetch the object data and update its state.

        Returns:
            DataInstance
        """
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

    def __reduce__(self):
        """Speciaization to replace with subworker.unpickle_input_object
        in Python task args while (cloud)pickling."""
        from . import pycode
        from ..subworker import subworker
        if pycode._global_pickle_inputs is None:
            # call normal __reduce__
            return super().__reduce__()
        base_name, counter, inputs, input_proto = pycode._global_pickle_inputs
        input_name = "{}{{{}}}".format(base_name, counter)
        pycode._global_pickle_inputs[1] += 1
        inputs.append((input_name, self))
        return (subworker.unpickle_input_object,
                (input_name, len(inputs) - 1,
                 input_proto.load, input_proto.content_type))

    def __repr__(self):
        t = " [D]" if self.data_type == DataType.DIRECTORY else ""
        return "<DObj {}{} id={} {}>".format(
            self.label, t, self.id, self.attributes)


def blob(value, label="const", content_type=None, encode=None, session=None):
    """
    Create a constant data object with accompanying data.

    Given `value` may be either `bytes` or any object to be encoded with
    `encoding` content type. Strings are encoded with utf-8 by default.
    Specify at most one of `content_type` and `encode`.
    """

    if content_type is not None:
        if encode is not None:
            raise RainException("Specify only one of content_type and encode")
        if not isinstance(value, bytes):
            raise RainException("content_type only allowed for `bytes`")

    if encode is None and isinstance(value, str):
        encode = "text:utf-8"
        if content_type is not None:
            raise RainException("content_type not allowed for `str`, use `encode=...`")

    if encode is not None:
        check_content_type(encode)
        value = encode_value(value, content_type=encode)
        content_type = encode

    if not isinstance(value, bytes):
        raise RainException(
            "Invalid blob type (only str or bytes are allowed without `encode`)")

    dataobj = DataObject(label, content_type=content_type, session=session)
    dataobj.data = value
    return dataobj


def pickled(val, label="pickle"):
    """
    Create a data object with pickled `val`.

    A shorthand for `blob(val, ancode='pickle')`.
    The default label is "pickle".
    """
    return blob(val, encode='pickle', label=label)


def directory(path=None, label="const_dir"):
    f = io.BytesIO()
    tf = tarfile.open(fileobj=f, mode="w")
    tf.add(path, ".")
    tf.close()
    data = f.getvalue()
    dataobj = DataObject(label, data_type=DataType.DIRECTORY)
    dataobj.data = data
    return dataobj


def to_data(obj):
    """Convert an object to DataObject/DataObjectPart"""
    if isinstance(obj, DataObject):
        return obj
    from .task import Task
    if isinstance(obj, Task):
        if len(obj.outputs) == 1:
            return obj.outputs[0]
        if len(obj.outputs) == 0:
            raise RainException("{} does not have any output".format(obj))
        else:
            raise RainException("{} returns multiple outputs".format(obj))

    if isinstance(obj, str) or isinstance(obj, bytes):
        raise RainException(
            "Instance of {!r} cannot be used as a data object.\n"
            "Hint: Wrap it with `blob` to use it as data object."
            .format(type(obj)))

    raise RainException(
            "Instance of {!r} cannot be used as a data object.\n"
            "Hint: Wrap it with `pickled` or `blob(encode=...)` to use it as a data object."
            .format(type(obj)))
