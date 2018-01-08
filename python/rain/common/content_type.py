from . import RainException
import pickle


def check_content_type(name):
    if name in set([None, "", "pickle", "json", "dir", "text", "cbor",
                    "protobuf"]):
        return True
    if (name.startswith("text:") or
       name.startswith("user:") or
       name.startswith("mime:") or
       name.startswith("protobuf:")):
        return True
    raise ValueError("Content type '{:r}' is not recognized".format(name))


def merge_content_types(name_a, name_b):
    """
    Check the names and return a common type.
    Raises `RainException` on mismatch.
    """
    check_content_type(name_a)
    check_content_type(name_b)
    if name_a is None or name_b.startswith(name_a):
        return name_b
    if name_b is None or name_a.startswith(name_b):
        return name_a
    raise RainException("Incompatible content types: {:r} and {:r}"
                        .format(name_a, name_b))


def encode_value(val, content_type):
    "Encodes given python object with `content_type`. Returns `EncodedBytes`."
    check_content_type(content_type)
    assert isinstance(content_type, str), "can't encode content_type `None`"

    if content_type == "pickle":
        d = pickle.dumps(val)
    elif content_type == "json":
        import json
        d = json.dumps(val).encode("utf-8")
    elif content_type == "cbor":
        import cbor
        d = cbor.dumps(val)
    elif content_type.startswith("text"):
        assert isinstance(val, str)
        if content_type == "text":
            enc = "utf-8"
        else:
            enc = content_type.split(":", maxsplit=1)[1]
        d = val.encode(enc)
    else:
        raise RainException("Encoding into {:r} unsupported"
                            .format(content_type))

    return EncodedBytes(d, content_type=content_type)


def decode_value(data, content_type):
    """
    Decodes given `bytes` into python object with `content_type`.

    Also accepts `EncodedBytes` but still requires `content_type`,
    use `EncodedBytes.load()` for a shorthand.
    """
    check_content_type(content_type)
    assert isinstance(data, bytes)
    assert isinstance(content_type, str), "can't encode content_type `None`"

    if content_type == "pickle":
        return pickle.loads(data)
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
        raise RainException("Decoding from {:r} unsupported"
                            .format(content_type))


class EncodedBytes(bytes):
    "Bytes type with `load` method (with given content_type)"

    def __new__(cls, data, content_type=None):
        b = bytes.__new__(cls, data)
        b.content_type = content_type
        return b

    def load(self):
        return decode_value(self, self.content_type)
