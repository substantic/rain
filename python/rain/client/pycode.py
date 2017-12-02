import cloudpickle
import inspect
import pickle
import contextlib
from collections import OrderedDict

from .task import Task
from .data import blob, DataObject, DataObjectPart
from .session import get_active_session
from .common import RainException

class InputObjectPlaceholder:
    def __init__(self, name, number):
        self.name = name
        self.number = number 
    def __repr__(self):
        return "InputObjectPlaceholder({!r}, {})".format(self.name, self.number)   


# Base name of current argument and growing list of input data objects
# while Py task arguments are pickled. `[arg_base_name, counter, inputs_list]`

_global_pickle_inputs = None

@contextlib.contextmanager
def _pickle_inputs_context(name, inputs):
    """Context manager to store current argument name and growing input objects list
    while Py task arguments are unpickled. Internal, not thread safe, not reentrant."""
    global _global_pickle_inputs
    assert _global_pickle_inputs is None
    _global_pickle_inputs = [name, 0, inputs]
    yield
    _global_pickle_inputs = None



def py_call(fn, inputs):
    return Task("py", inputs)


def py_obj(obj, label=""):
    if isinstance(obj, DataObject):
        return obj
    if isinstance(obj, DataObjectPart):
        return obj
    return blob(cloudpickle.dumps(obj), label)


def remote(outputs=1, auto_load=None, pickle_outputs=False):
    # TODO: use pickle_outputs
    def make_remote(fn):
        if not inspect.isfunction(fn):
            raise RainException("remote() arg {!r} is not a function".format(fn))
        def make_task(*args, **kwargs):
            session = get_active_session()
            fn_blob = session.static_data.get(fn)
            if fn_blob is None:
                fn_blob = blob(cloudpickle.dumps(fn), fn.__name__)
                fn_blob.keep()
                session.static_data[fn] = fn_blob
            inputs = [fn_blob]
            sig = inspect.signature(fn)
            # Check the parameter compatibility for fn and bind names <-> values
            # First args is the context
            bound = sig.bind(None, *args, **kwargs)
            # Pickle positional args
            pickled_args = []
            for i, argval in enumerate(args):
                # Within this session state, the DataObjects are seialized as InputPlaceholders
                with _pickle_inputs_context("args[{}]".format(i), inputs):
                    pickled_args.append(cloudpickle.dumps(argval)) # TODO: better name
            # Pickle positional args
            pickled_kwargs = OrderedDict()
            for name, argval in kwargs.items():
                # Within this session state, the DataObjects are seialized as InputPlaceholders
                with _pickle_inputs_context(name, inputs):
                    pickled_kwargs[name] = cloudpickle.dumps(argval)
            task_config = {
                'args': pickled_args,
                'kwargs': pickled_kwargs,
                'auto_load': auto_load,
                'outputs': outputs, # TODO: somehow check and pre-process?
            }
            print(repr(task_config))
            return Task("py", pickle.dumps(task_config), inputs, outputs)
        return make_task
    return make_remote
