
import cloudpickle
import os
from rain.client.rpc import common as rpc_common
from ..common.attributes import attributes_from_capnp
from ..common.attributes import attributes_to_capnp


def data_from_capnp(reader):
    which = reader.storage.which()
    if which == "memory":
        result = MemoryBlob(reader.storage.memory)
    elif which == "path":
        result = FileBlob(reader.storage.path)
    else:
        raise Exception("Invalid storage type")
    result.attributes = attributes_from_capnp(reader.attributes)
    return result


class Data:
    load_cache = None

    # Contains object id if the object is known to worker
    worker_object_id = None


class Blob(Data):

    def load(self, cache=False):
        if self.load_cache is not None:
            return self.load_cache
        obj = cloudpickle.loads(self.to_bytes())
        if cache:
            self.load_cache = obj
        return obj


class MemoryBlob(Blob):

    def __init__(self, data):
        self.data = bytes(data)
        self.attributes = {}

    def to_bytes(self):
        return self.data

    def to_str(self):
        # TODO: Check attributes for encoding
        return self.to_bytes().decode()

    def to_capnp(self, builder):
        builder.type = rpc_common.DataObjectType.blob
        if self.worker_object_id:
            builder.storage.init("inWorker")
            builder.storage.inWorker.sessionId = self.worker_object_id[0]
            builder.storage.inWorker.id = self.worker_object_id[1]
        else:
            builder.storage.memory = self.data
        attributes_to_capnp(self.attributes, builder.attributes)

    def __repr__(self):
        return "{}({}: {!r})".format(self.__class__.__name__,
                                     len(self.data), self.data[:30])


class FileBlob(Blob):

    def __init__(self, filename):
        self.filename = filename
        self.attributes = {}

    def to_bytes(self):
        with open(self.filename, "rb") as f:
            return f.read()

    def to_str(self):
        # TODO: Check attributes for encoding
        with open(self.filename, "r") as f:
            return f.read()

    def to_capnp(self, builder):
        builder.type = rpc_common.DataObjectType.blob
        if self.worker_object_id:
            builder.storage.init("inWorker")
            builder.storage.inWorker.sessionId = self.worker_object_id[0]
            builder.storage.inWorker.id = self.worker_object_id[1]
        else:
            builder.storage.path = self.filename
        attributes_to_capnp(self.attributes, builder.attributes)

    def _remove(self):
        os.unlink(self.filename)
