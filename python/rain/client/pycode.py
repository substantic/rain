import cloudpickle
from .task import Task
from .data import blob, DataObject, DataObjectPart


def py_call(fn):
    return Task("py", cloudpickle.dumps(fn))


def py_obj(obj, label=""):
    if isinstance(obj, DataObject):
        return obj
    if isinstance(obj, DataObjectPart):
        return obj
    return blob(cloudpickle.dumps(obj), label)


def remote():
    def make_remote(fn):
        def make_task():
            return py_call(fn)
        return make_task
    return make_remote
