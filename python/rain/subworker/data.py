
import cloudpickle


def data_from_capnp(reader):
    return MemoryBlob(reader.storage.memory)


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

    def to_str(self):
        # TODO: Check additionals for encoding
        self.to_bytes().decode()


class MemoryBlob(Blob):

    def __init__(self, data):
        self.data = bytes(data)

    def to_bytes(self):
        return self.data
