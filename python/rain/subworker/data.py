
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
        builder.storage.memory = self.data


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
        builder.storage.path = self.filename

    def _remove(self):
        os.unlink(self.filename)
