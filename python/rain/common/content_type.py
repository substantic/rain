from . import RainException
import cloudpickle
import pickle


def check_content_type(name):
    if name in [None, "pickle", "json", "dir", "text", "cbor",
                "protobuf", "cloudpickle"]:
        return True
    if (name.startswith("text:") or
       name.startswith("user:") or
       name.startswith("mime:") or
       name.startswith("protobuf:")):
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


def encode_value(val, content_type):
    "Encodes given python object with `content_type`. Returns `EncodedBytes`."
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
    elif content_type.startswith("text"):
        if not isinstance(val, str):
            raise RainException("Encoding {!r} can only encode `str` objects."
                                .format(content_type))
        if content_type == "text":
            enc = "utf-8"
        else:
            enc = content_type.split(":", maxsplit=1)[1]
        d = val.encode(enc)
    else:
        raise RainException("Encoding into {!r} unsupported"
                            .format(content_type))

    return EncodedBytes(d, content_type=content_type)


def decode_value(data, content_type):
    """
    Decodes given `bytes` into python object with `content_type`.

    Also accepts `EncodedBytes` but still requires `content_type`,
    use `EncodedBytes.load()` for a shorthand.
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
    elif content_type.startswith("text"):
        if content_type == "text":
            enc = "utf-8"
        else:
            enc = content_type.split(":", maxsplit=1)[1]
        return data.decode(enc)
    else:
        raise RainException("Decoding from {!r} unsupported"
                            .format(content_type))


class EncodedBytes(bytes):
    "Bytes type with `load` method (with given content_type)"

    def __new__(cls, data, content_type=None):
        b = bytes.__new__(cls, data)
        b.content_type = content_type
        return b

    def load(self):
        return decode_value(self, self.content_type)
