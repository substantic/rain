import cloudpickle
import inspect
import pickle
import contextlib
import time
from collections import OrderedDict

from .task import Task
from .data import blob, DataObject, DataObjectPart
from .session import get_active_session
from .common import RainException, RainWarning


PICKLE_ARG_SIZE_LIMIT = 256 * 1024
PICKLE_ARG_TIME_LIMIT = 1.0


# Base name of current argument and growing list of input data objects
# while Py task arguments are pickled. `[arg_base_name, counter, inputs_list]`
_global_pickle_inputs = None


@contextlib.contextmanager
def _pickle_inputs_context(name, inputs):
    """Context manager to store current argument name and growing input
    objects list while Py task arguments are unpickled. Internal, not
    thread safe, not reentrant."""
    global _global_pickle_inputs
    assert _global_pickle_inputs is None
    _global_pickle_inputs = [name, 0, inputs]
    try:
        yield
    finally:
        _global_pickle_inputs = None


def _checked_pickle(d, name=None):
    """Perform fast pickle.dumps or (for cmplex objects)
    cloudpickle.dumps and issue a warning if the result is
    unexpectedly big (PICKLE_ARG_SIZE_LIMIT) or it takes too
    long (PICKLE_ARG_TIME_LIMIT)."""
    t0 = time.clock()
    try:
        p = pickle.dumps(d)
    except pickle.PicklingError:
        p = cloudpickle.dumps(d)
    except AttributeError:
        p = cloudpickle.dumps(d)
    if len(p) > PICKLE_ARG_SIZE_LIMIT:
        raise RainWarning(
            "Pickled object {} length {} > PICKLE_ARG_SIZE_LIMIT={}. \
Consider using a blob() for the data."
            .format(name or '<unknown>', len(d), PICKLE_ARG_SIZE_LIMIT))
    if time.clock() - t0 > PICKLE_ARG_TIME_LIMIT:
        raise RainWarning(
            "Pickling object {} took {} s > PICKLE_ARG_TIME_LIMIT={}. \
Consider using a blob() for the data."
            .format(name or '<unknown>', len(d), PICKLE_ARG_TIME_LIMIT))
    return p


# TODO: (gavento): Deprecate or upgrade to complex args (as wrapper for remote)
def py_call(fn, inputs):
    return Task("py", inputs)


def py_obj(obj, label=""):
    if isinstance(obj, DataObject):
        return obj
    if isinstance(obj, DataObjectPart):
        return obj
    return blob(cloudpickle.dumps(obj), label)


def remote(outputs=1, auto_load=None, pickle_outputs=False):
    # TODO: (gavento) use pickle_outputs and outputs spec
    def make_remote(fn):

        def make_task(*args, **kwargs):
            session = get_active_session()
            fn_blob = session.static_data.get(fn)
            if fn_blob is None:
                d = _checked_pickle(fn, fn.__name__)
                fn_blob = blob(d, fn.__name__)
                fn_blob.keep()
                session.static_data[fn] = fn_blob
            inputs = [fn_blob]
            sig = inspect.signature(fn)
            # Check the parameter compatibility for fn
            # Note that the first arg is the context
            sig.bind(None, *args, **kwargs)
            # Pickle positional args
            pickled_args = []
            for i, argval in enumerate(args):
                # Within this session state, the DataObjects are seialized as
                # InputPlaceholders
                name = "arg{}".format(i)
                # TODO: (gavento) construct a better name
                with _pickle_inputs_context(name, inputs):
                    d = _checked_pickle(argval, name=name)
                    pickled_args.append(d)
            # Pickle positional args
            pickled_kwargs = OrderedDict()
            for name, argval in kwargs.items():
                # Within this session state, the DataObjects are seialized as
                # InputPlaceholders
                with _pickle_inputs_context(name, inputs):
                    d = _checked_pickle(argval)
                    pickled_kwargs[name] = d
            task_config = {
                'args': pickled_args,
                'kwargs': pickled_kwargs,
                'auto_load': auto_load,
                'outputs': outputs,
            }
            return Task("py", pickle.dumps(task_config), inputs, outputs)

        if not inspect.isfunction(fn):
            raise RainException(
                "remote() arg {!r} is not a function".format(fn))
        return make_task
    return make_remote
