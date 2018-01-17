def worker_id_from_capnp(reader):
    if reader.address.which() == "ipv4":
        address = reader.address.ipv4
    elif reader.address.which() == "ipv6":
        raise Exception("Not implemented")
    else:
        raise Exception("Unknown address")
    return "{}:{}".format(".".join(map(str, address)), reader.port)
