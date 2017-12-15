
import json


def attributes_from_capnp(attributes):
    return {item.key: (json.loads(item.value))
            for item in attributes.items}


def attributes_to_capnp(attributes, builder):
    items = builder.init("items", len(attributes))
    for i, item in enumerate(attributes.items()):
        items[i].key = item[0]
        items[i].value = json.dumps(item[1])
