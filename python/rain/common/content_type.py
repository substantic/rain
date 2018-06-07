import pickle

import cloudpickle

from .errors import RainException


def check_content_type(name):
    if name in [None, "", "pickle", "json", "dir", "text", "cbor", "arrow",
                "protobuf", "cloudpickle"]:
        return True
    if (name.startswith("text-") or
       name.startswith("user/") or
       name.startswith("mime/") or
       name.startswith("protobuf/")):
        return True
    raise ValueError("Content type {!r} is not recognized".format(name))


def merge_content_types(name_a, name_b):
    """
    Check the names and return a common type.
    Raises `RainException` on mismatch.
    """
    check_content_type(name_a)
    check_content_type(name_b)
    if name_a is None and name_b is None:
        return None
    if name_a is None or (name_b is not None and name_b.startswith(name_a)):
        return name_b
    if name_b is None or name_a.startswith(name_b):
        return name_a
    # Special case of pickle/cloudpickle
    if sorted([name_a, name_b]) == ["cloudpickle", "pickle"]:
        return "cloudpickle"
    raise RainException("Incompatible content types: {!r} and {!r}"
                        .format(name_a, name_b))


def is_type_instance(t, ctype):
    "Return whether content type `t` is a subtype of `ctype`."
    check_content_type(t)
    check_content_type(ctype)
    if ctype is None or t == ctype:
        return True
    if t is not None and t.startswith(ctype):
        return True
    if t == 'pickle' and ctype == 'cloudpickle':
        return True
    return False


def encode_value(val, content_type):
    "Encodes given python object with `content_type`. Returns `bytes`."
    check_content_type(content_type)
    if content_type is None:
        raise RainException("can't encode None content_type")

    if content_type == "pickle":
        d = pickle.dumps(val)
    elif content_type == "cloudpickle":
        d = cloudpickle.dumps(val)
    elif content_type == "json":
        import json
        d = json.dumps(val).encode("utf-8")
    elif content_type == "cbor":
        import cbor
        d = cbor.dumps(val)
    elif content_type == "arrow":
        import pyarrow
        d = pyarrow.serialize(val).to_buffer().to_pybytes()
    elif content_type.startswith("text"):
        if not isinstance(val, str):
            raise RainException("Encoding {!r} can only encode `str` objects."
                                .format(content_type))
        if content_type == "text":
            enc = "utf-8"
        else:
            enc = content_type.split("-", maxsplit=1)[1]
        d = val.encode(enc)
    else:
        raise RainException("Encoding into {!r} unsupported"
                            .format(content_type))

    assert isinstance(d, bytes)
    return d


def decode_value(data, content_type):
    """
    Decodes given `bytes` into python object with `content_type`.
    """
    check_content_type(content_type)
    if content_type is None:
        raise RainException("can't decode None content_type")
    if not isinstance(data, bytes):
        raise RainException("can only decode `bytes` values")

    if content_type == "pickle":
        # NOTE: Should we use cloudpickle.loads even for pickle data?
        return pickle.loads(data)
    elif content_type == "cloudpickle":
        return cloudpickle.loads(data)
    elif content_type == "json":
        import json
        return json.loads(data.decode("utf-8"))
    elif content_type == "cbor":
        import cbor
        return cbor.loads(data)
    elif content_type == "arrow":
        import pyarrow
        return pyarrow.deserialize(data)
    elif content_type.startswith("text"):
        if content_type == "text":
            enc = "utf-8"
        else:
            enc = content_type.split("-", maxsplit=1)[1]
        return data.decode(enc)
    else:
        raise RainException("Decoding from {!r} unsupported"
                            .format(content_type))
