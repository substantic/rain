
import cloudpickle
import os
from ..common.attributes import attributes_from_capnp
from ..common.attributes import attributes_to_capnp


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
    instance = DataInstance(data, path)
    instance.attributes = attributes_from_capnp(reader.attributes)
    return instance


class DataInstance:

    # Cache for python deseriazed version of the object
    load_cache = None

    # Contains object id if the object is known to worker
    worker_object_id = None

    path = None
    data = None

    def __init__(self, data=None, path=None):
        assert data is not None or path is not None
        if data is not None:
            self.data = bytes(data)
        if path is not None:
            self.path = path
        self.attributes = {}

    def load(self, cache=False):
        """Load object according content type"""
        if self.load_cache is not None and cache:
            return self.load_cache
        obj = cloudpickle.loads(self.to_bytes())
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
