import io
import json
import tarfile

import capnp

from ..common import ID, DataType, RainException
from ..common.attributes import ObjectSpec
from ..common.content_type import (check_content_type, encode_value,
                                   merge_content_types)
from .session import get_active_session


class DataObject:

    # Flag if data object should be kept on server
    _keep = False

    # State of object
    # None = Not submitted
    _state = None

    # Value of data object (value can be filled by client if it is constant,
    # or by fetching from server)
    _data = None

    def __init__(self, label=None, session=None, data_type=DataType.BLOB, content_type=None):
        assert isinstance(data_type, DataType)
        if session is None:
            session = get_active_session()
        self._session = session
        self._spec = ObjectSpec()
        self._info = None
        self._spec.label = label
        self._spec.id = session._register_dataobj(self)
        assert isinstance(self.id, ID)
        self._spec.content_type = content_type
        self._spec.data_type = data_type

    @property
    def id(self):
        """Getter for object ID."""
        return self._spec.id

    @property
    def state(self):
        """Getter for object state."""
        return self._state

    @property
    def content_type(self):
        """Final content type (if known and present) or just the spec content_type."""
        if self._info:
            return merge_content_types(self._spec.content_type, self._info.content_type)
        return self._spec.content_type

    @property
    def spec(self):
        """Getter for object specification. Read only!"""
        return self._spec

    @property
    def info(self):
        """Getter for object information. Read only in client!
           In remote tasks only `user` is writable."""
        return self._info

    def _free(self):
        """Set flag that object is not available on the server """
        self._keep = False

    def unkeep(self):
        """Remove data object from the server"""
        self._session.unkeep((self,))

    def keep(self):
        """Set flag that is object should be kept on the server"""
        if self.state is not None:
            raise RainException("cannot keep submitted data object {!r}".format(self))
        self._keep = True

    def is_kept(self):
        """Returns the value of self._keep"""
        return self._keep

    def _to_capnp(self, out):
        out.spec = json.dumps(self._spec._to_json())
        out.keep = self._keep

        if self._data is not None:
            out.data = self._data
            out.hasData = True
        else:
            out.hasData = False

    def wait(self):
        self._session.wait((self,))

    def fetch(self):
        """
        Fetch the object data and update its state.

        Returns:
            DataInstance
        """
        return self._session.fetch(self)

    def expect_dir(self):
        """Raise TypeError if the DataObject is not a directory data-type."""
        if self.spec.data_type != DataType.DIRECTORY:
            raise TypeError("Directory expected.")

    def update(self):
        self._session.update((self,))

    def __del__(self):
        if self.state is not None and self._keep:
            try:
                self._session.client._unkeep((self,))
            except capnp.lib.capnp.KjException:
                # Ignore capnp exception, since this constructor may be
                # called when connection is closed
                pass

    def __reduce__(self):
        """Speciaization to replace with executor.unpickle_input_object
        in Python task args while (cloud)pickling."""
        from . import pycode
        from ..executor import executor
        if pycode._global_pickle_inputs is None:
            # call normal __reduce__
            return super().__reduce__()
        base_name, counter, inputs, input_proto = pycode._global_pickle_inputs
        input_name = "{}{{{}}}".format(base_name, counter)
        pycode._global_pickle_inputs[1] += 1
        inputs.append((input_name, self))
        return (executor.unpickle_input_object,
                (input_name, len(inputs) - 1,
                 input_proto.load, input_proto.content_type))

    def __repr__(self):
        t = " [D]" if self.spec.data_type == DataType.DIRECTORY else ""
        return "<DObj {}{} id={} {}>".format(
            self.spec.label, t, self.id, self.spec)


def blob(value, label="const", content_type=None, encode=None):
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
        encode = "text"
        if content_type is not None:
            raise RainException("content_type not allowed for `str`, use `encode=...`")

    if encode is not None:
        check_content_type(encode)
        value = encode_value(value, content_type=encode)
        content_type = encode

    if not isinstance(value, bytes):
        raise RainException(
            "Invalid blob type (only str or bytes are allowed without `encode`)")

    dataobj = DataObject(label, content_type=content_type)
    dataobj._data = value
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
    dataobj._data = data
    return dataobj


def to_dataobj(obj):
    """Convert an object to DataObject/DataObjectPart"""
    if isinstance(obj, DataObject):
        return obj

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


from .task import Task  # noqa