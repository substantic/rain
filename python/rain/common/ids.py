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


def id_from_capnp(reader):
    return ID(session_id=reader.sessionId, id=reader.id)


def id_to_capnp(obj, builder):
    builder.sessionId = obj.session_id
    builder.id = obj.id


def governor_id_from_capnp(reader):
    if reader.address.which() == "ipv4":
        address = reader.address.ipv4
    elif reader.address.which() == "ipv6":
        raise Exception("Not implemented")
    else:
        raise Exception("Unknown address")
    return "{}:{}".format(".".join(map(str, address)), reader.port)
