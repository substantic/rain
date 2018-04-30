import enum


class DataType(enum.Enum):

    BLOB = "blob"
    DIRECTORY = "directory"

    def to_capnp(self):
        return self.value

    @classmethod
    def from_capnp(cls, value):
        return DataType(value)