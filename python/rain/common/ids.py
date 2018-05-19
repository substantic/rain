from collections import namedtuple


ID = namedtuple("ID", ["session_id", "id"])
ID.__repr__ = lambda self: "{}:{}".format(self[0], self[1])
ID.__doc__ = """
A rain task and object ID. A named tuple `(session_id, id)`.
"""


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
