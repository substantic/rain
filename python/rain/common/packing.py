
import pickle
import json


def find_packer(content_type):
    """Returns (packer, is_binary) where 'packer' is object with methods
       load/dump/loads/dumps, 'is_binary' is True if input is bytes, and False
       if input is str.
    """
    if content_type == "py":
        return (pickle, True)
    if content_type == "json":
        return (json, False)
    raise Exception("Unknown content type '{}' to pack/unpack"
                    .format(content_type))


def dump_mem(obj, content_type):
    packer, is_binary = find_packer(content_type)
    result = packer.dumps(obj)
    if is_binary:
        return result
    else:
        return result.encode()


def dump_file(obj, filename, content_type):
    packer, is_binary = find_packer(content_type)
    mode = "wb" if is_binary else "w"
    with open(filename, mode) as f:
        packer.dump(obj, f)


def load_mem(data, content_type):
    packer, is_binary = find_packer(content_type)
    if not is_binary:
        data = data.decode()
    return packer.loads(data)


def load_file(filename, content_type):
    packer, is_binary = find_packer(content_type)
    mode = "rb" if is_binary else "r"
    with open(filename, mode) as f:
        return packer.load(f)
