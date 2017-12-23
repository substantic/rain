
from .data import DataObject


class Output:

    def __init__(self, label, path=None):
        self.label = label
        if path is None:
            if label:
                path = label
            else:
                path = "output_{}".format(id(self))
        self.path = path

    def make_data_object(self):
        return DataObject(self.label)

    def __repr__(self):
        if self.path is None:
            return "<Output {} path={}>".format(self.label, self.path)
        else:
            return "<Output {}>".format(self.label)


def to_output(obj):
    if isinstance(obj, Output):
        return obj
    if isinstance(obj, str):
        return Output(obj)
    raise Exception("Object {!r} cannot be used as output".format(obj))
