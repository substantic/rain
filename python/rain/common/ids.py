from collections import namedtuple

_ID = namedtuple("_ID", ["session_id", "id"])


class ID(_ID):
    """A rain task and object ID. A named tuple `(session_id, id)`."""

    def __repr__(self):
        return "{}/{}".format(self[0], self[1])

    @classmethod
    def _from_json(cls, data):
        """Convert from a tuple `[s_id, id]`. No checking."""
        return cls(data[0], data[1])

    def _to_json(self):
        return [self[0], self[1]]
