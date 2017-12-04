

def value_from_capnp(item):
    which = item.which()
    if which == "str":
        return item.str
    elif which == "int":
        return item.int
    elif which == "float":
        return item.float
    elif which == "bool":
        return item.bool
    elif which == "data":
        return item.data
    else:
        raise Exception("Invalid type of additional")


def value_to_capnp(obj, builder):
    if isinstance(obj, str):
        builder.str = obj
    elif isinstance(obj, bool):  # !!! bool has to be before test to int
        builder.bool = obj
    elif isinstance(obj, int):
        builder.int = obj
    elif isinstance(obj, float):
        builder.float = obj
    elif isinstance(obj, bytes):
        builder.data = obj
    else:
        raise Exception("Invalid type of additional")


def additionals_from_capnp(additionals):
    return {item.key: value_from_capnp(item.value)
            for item in additionals.items}


def additionals_to_capnp(additionals, builder):
    items = builder.init("items", len(additionals))
    for i, item in enumerate(additionals.items()):
        items[i].key = item[0]
        value_to_capnp(item[1], items[i].value)
