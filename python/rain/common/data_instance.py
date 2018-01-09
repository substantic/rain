
import os

from .attributes import attributes_from_capnp
from .attributes import attributes_to_capnp
from .content_type import decode_value, merge_content_types
from . import RainException
from .utils import format_size


class DataInstance:
    """
    Instance of Data object with data or file reference.

    This serves as a proxy to a *finished* `DataObject`.
    The class is used in a python task in subworkers and as a result
    of `DataObject.fetch`.

    The user should not manually create this object, but always use `fetch()`
    or a method on python task context.
    """

    # The data object when at client
    _data_object = None

    # Cache for python deseriazed version of the object
    _load_cache = None

    # Cache for received/read bytes
    _data = None

    # Contains object id if the object is known to worker
    _object_id = None

    # The file path in case the data is in a file
    _path = None

    # The same semantics as parent DO attributes
    # (whether parent is present or not)
    # Will always at least contain "config" and "content_type" under "config".
    attributes = {}

    def __init__(self, *,
                 data=None,
                 path=None,
                 data_object=None,
                 content_type=None,
                 attributes=None,
                 object_id=None):
        if (path is None) == (data is None):
            raise RainException("provide either `data` or `path`")
        if data is not None:
            assert isinstance(data, bytes)
            self._data = data
        else:
            self._path = path

        if data_object is not None:
            # At client
            assert attributes is None
            assert content_type is None
            assert object_id is None
            self._data_object = data_object
            self.attributes = data_object.attributes
            self._object_id = data_object.id_pair()
            assert "content_type" in self.attributes["config"]
        else:
            self._object_id = object_id
            self.attributes = attributes if attributes is not None else {}
            self.attributes.setdefault("config", {})
            self.attributes["config"]["content_type"] = merge_content_types(
                self.attributes["config"].get("content_type"), content_type)

    @property
    def content_type(self):
        return self.attributes["config"]["content_type"]

    def load(self, cache=False):
        """
        Load object according content type, optionally caching the result.

        If you set `cache=True`, you must not modify the returned object as it
        may be shared between loads or even tasks. With `cache=False`, you get
        a new copy every time.
        """
        if self._load_cache is not None and cache:
            return self._load_cache
        if self._data:
            obj = decode_value(self._data, self.content_type)
        else:
            with open(self._path, "rb") as f:
                obj = decode_value(f.read(), self.content_type)
        if cache:
            self._load_cache = obj
        return obj

    def get_bytes(self):
        """
        Return the data as `bytes`. May read them from the disk.
        """
        if self._data is not None:
            return self._data
        else:
            with open(self._path, "rb") as f:
                return f.read()

    def _to_capnp(self, builder):
        "Internal serializer."
        if self._object_id:
            builder.storage.init("inWorker")
            builder.storage.inWorker.sessionId = self._object_id[0]
            builder.storage.inWorker.id = self._object_id[1]
        elif self.path:
            builder.storage.path = self.path
        else:
            builder.storage.memory = self.data
        attributes_to_capnp(self.attributes, builder.attributes)

    @classmethod
    def _from_capnp(cls, reader):
        "Internal deserializer."
        which = reader.storage.which()
        data = None
        path = None
        if which == "memory":
            data = reader.storage.memory
        elif which == "path":
            path = reader.storage.path
        else:
            raise Exception("Invalid storage type")
        attributes = attributes_from_capnp(reader.attributes)
        return cls(data=data, path=path, attributes=attributes)

    def __repr__(self):
        if self._data:
            return "<DataInstance {}>".format(format_size(len(self._data)))
        else:
            return "<DataInstance {!r}>".format(self._path)

    def _remove(self):
        assert self._path
        os.unlink(self._path)
