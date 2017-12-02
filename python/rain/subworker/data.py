
import cloudpickle
import os
from rain.client.rpc import common as rpc_common


def data_from_capnp(reader):
    which = reader.storage.which()
    if which == "memory":
        return MemoryBlob(reader.storage.memory)
    if which == "path":
        return FileBlob(reader.storage.path)
    raise Exception("Invalid storage type")


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

    def to_bytes(self):
        return self.data

    def to_str(self):
        # TODO: Check additionals for encoding
        self.to_bytes().decode()

    def to_capnp(self, builder):
        builder.type = rpc_common.DataObjectType.blob
        if self.worker_object_id:
            builder.storage.init("inWorker")
            builder.storage.inWorker.sessionId = self.worker_object_id[0]
            builder.storage.inWorker.id = self.worker_object_id[1]
        else:
            builder.storage.memory = self.data

    def __repr__(self):
        return "{}({}: {!r})".format(self.__class__.__name__,
                                     len(self.data), self.data[:30])


class FileBlob(Blob):

    def __init__(self, filename):
        self.filename = filename

    def to_bytes(self):
        with open(self.filename, "rb") as f:
            return f.read()

    def to_str(self):
        # TODO: Check additionals for encoding
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

    def _remove(self):
        os.unlink(self.filename)
