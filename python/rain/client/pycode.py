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


def remote():
    def make_remote(fn):
        def make_task(*args):
            """TODO enable when server is fixed
            client = get_active_session().client
            fn_blob = client.get_static_data(fn)
            if fn_blob is None:
                client.set_static_blob(
                    fn, cloudpickle.dumps(fn), "Fn:" + fn.__name__)
                fn_blob = client.get_static_data(fn)
            """
            fn_blob = blob(cloudpickle.dumps(fn), fn.__name__)  # Just heck until bug in server is not fixed
            inputs = (fn_blob,) + args
            return Task("py", None, inputs)
        return make_task
    return make_remote
