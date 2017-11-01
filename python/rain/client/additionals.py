

def value_from_capnp(item):
    which =  item.which()
    if which == "str":
        return item.str
    else:
        raise Exception("Not implemented yet")


def additionals_from_capnp(additionals):
    return {item.key: value_from_capnp(item.value) for item in additionals.items}