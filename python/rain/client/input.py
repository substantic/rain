from .data import to_data


class Input:

    data = None

    def __init__(self, label=None, path=None, data=None):
        if label is not None and not isinstance(label, str):
            raise Exception("Label has to be string, not {!r}".format(label))
        self.label = label
        if path is None:
            if label:
                path = label
            else:
                path = "input_{}".format(id(self))
        self.path = path
        if data is not None:
            self.data = to_data(data)

    def __repr__(self):
        args = []
        if self.path:
            args.append("path={}".format(self.path))
        if self.data:
            args.append("data={}".format(self.data))
        return "<Input '{}'>".format(self.label, " ".join(args))


def to_input(obj):
    if isinstance(obj, Input):
        if obj.data:
            raise Exception(
                "Input without 'data' is expected, got {!r}".format(obj))
        return obj
    elif isinstance(obj, str):
        return Input(obj)
    raise Exception("Object {!r} cannot be used as input".format(obj))


def to_input_with_data(obj, label=None):
    if isinstance(obj, Input):
        if not obj.data:
            raise Exception(
                "Input with 'data' is expected, got {!r}".format(obj))
        return obj
    return Input(label=label, data=obj)
