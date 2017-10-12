import cloudpickle
from .task import Task
from .data import blob, DataObject, DataObjectPart
from .session import get_active_session


def py_call(fn, inputs):
    return Task("py", inputs)


def py_obj(obj, label=""):
    if isinstance(obj, DataObject):
        return obj
    if isinstance(obj, DataObjectPart):
        return obj
    return blob(cloudpickle.dumps(obj), label)


def remote(outputs=None):
    def make_remote(fn):
        def make_task(*args):
            session = get_active_session()
            fn_blob = session.static_data.get(fn)
            if fn_blob is None:
                fn_blob = blob(cloudpickle.dumps(fn), fn.__name__)
                fn_blob.keep()
                session.static_data[fn] = fn_blob
            inputs = (fn_blob,) + args
            return Task("py", None, inputs, outputs)
        return make_task
    return make_remote
