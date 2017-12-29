
import cloudpickle
import os
from ..common.attributes import attributes_from_capnp
from ..common.attributes import attributes_to_capnp
from ..common import packing


def data_from_capnp(reader):
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
    instance = DataInstance(data, path, attributes=attributes)
    return instance


class DataInstance:
    """
    Instance od Data object in subworker.

    The user should not create manually create this object,
    but always use method on context
    """

    # Cache for python deseriazed version of the object
    load_cache = None

    # Contains object id if the object is known to worker
    worker_object_id = None

    path = None
    data = None

    def __init__(self,
                 data=None,
                 path=None,
                 content_type=None,
                 attributes=None):
        assert data is not None or path is not None
        if data is not None:
            self.data = bytes(data)
        if path is not None:
            self.path = path
        if attributes is None:
            attributes = {}
        self.attributes = attributes
        if content_type is not None:
            attributes["config"] = {"content_type": content_type}

    @property
    def content_type(self):
        config = self.attributes.get("config")
        if config:
            return config.get("content_type")
        else:
            return None

    def load(self, cache=False):
        """Load object according content type"""
        if self.load_cache is not None and cache:
            return self.load_cache
        if self.data:
            obj = packing.load_mem(self.data, self.content_type)
        else:
            with open(self.path) as f:
                obj = packing.load_file(f, self.content_type)
        if cache:
            self.load_cache = obj
        return obj

    def to_bytes(self):
        if self.data is not None:
            return self.data
        else:
            with open(self.path, "rb") as f:
                return f.read()

    def to_str(self):
        # TODO: Check attributes for encoding
        if self.data is not None:
            return self.to_bytes().decode()
        else:
            with open(self.path, "r") as f:
                return f.read()

    def to_capnp(self, builder):
        if self.worker_object_id:
            builder.storage.init("inWorker")
            builder.storage.inWorker.sessionId = self.worker_object_id[0]
            builder.storage.inWorker.id = self.worker_object_id[1]
        elif self.path:
            builder.storage.path = self.path
        else:
            builder.storage.memory = self.data
        attributes_to_capnp(self.attributes, builder.attributes)

    def __repr__(self):
        return "<DataInstance>"

    def _remove(self):
        assert self.path
        os.unlink(self.path)
