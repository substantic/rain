

def value_from_capnp(item):
    which = item.which()
    if which == "str":
        return item.str
    else:
        raise Exception("value_from_capnp: Not implemented yet")


def value_to_capnp(obj, builder):
    if isinstance(obj, str):
        builder.str = obj
    else:
        raise Exception("value_to_capnp: Not implemented yet")


def additionals_from_capnp(additionals):
    return {item.key: value_from_capnp(item.value)
            for item in additionals.items}


def additionals_to_capnp(additionals, builder):
    items = builder.init("items", len(additionals))
    for i, item in enumerate(additionals.items()):
        items[i].key = item[0]
        value_to_capnp(item[1], items[i].value)
