from .common import RainException
import pickle


def check_content_type(name):
    if name in set([None, "", "pickle", "json", "dir", "text", "cbor", "protobuf"]):
        return True
    if (name.starts_with("text:") or
        name.starts_with("user:") or
        name.starts_with("mime:") or
        name.starts_with("protobuf:")):
            return True
    raise ValueError("Content type '{:r}' is not recognized".format(name))


def merge_content_types(name_a, name_b):
    "Check the names and return a common type, raising RainException on mismatch."
    check_content_type(name_a)
    check_content_type(name_b)
    if name_a is None or name_b.starts_with(name_a):
        return name_b
    if name_b is None or name_a.starts_with(name_b):
        return name_a
    raise RainException("Incompatible content types: {:r} and {:r}"
                        .format(name_a, name_b))


def encode_value(val, content_type):
    check_content_type(content_type)

    if content_type == "pickle":
        return pickle.dumps(val)
    elif content_type == "json":
        import json
        return json.dumps(val)
    elif content_type == "cbor":
        import cbor
        return cbor.dumps(val)
    elif content_type.starts_with("text"):
        assert isinstance(val, str)
        if content_type == "text":
            enc = "utf-8"
        else:
            enc = content_type.split(":", maxsplit=1)[1]
        return val.encode(enc)
    else:
        raise RainException("Encoding into {:r} unsupported"
                            .format(content_type))


def decode_value(data, content_type):
    check_content_type(content_type)
    assert isinstance(data, bytes)

    if content_type == "pickle":
        return pickle.loads(data)
    elif content_type == "json":
        import json
        return json.loadss(data)
    elif content_type == "cbor":
        import cbor
        return cbor.loads(data)
    elif content_type.starts_with("text"):
        if content_type == "text":
            enc = "utf-8"
        else:
            enc = .split(":", maxsplit=1)[1]
        return data.encode(enc)
    else:
        raise RainException("Decoding from {:r} unsupported"
                            .format(content_type))
