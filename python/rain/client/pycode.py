import inspect
import contextlib
import time
import base64
import cloudpickle
from collections import OrderedDict

from .task import Task, TaskBuilder
from .data import blob, DataObject
from .session import get_active_session
from ..common import RainException, RainWarning
from .input import Input

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


def _checked_cloudpickle(d, name=None):
    """Perform cloudpickle.dumps and issue a warning if the result is
    unexpectedly big (PICKLE_ARG_SIZE_LIMIT) or it takes too
    long (PICKLE_ARG_TIME_LIMIT)."""
    t0 = time.clock()
    p = cloudpickle.dumps(d)
    if len(p) > PICKLE_ARG_SIZE_LIMIT:
        raise RainWarning("Pickled object {} length {} > PICKLE_ARG_SIZE_LIMIT={}. " +
                          "Consider using a blob() for the data."
                          .format(name or '<unknown>', len(d), PICKLE_ARG_SIZE_LIMIT))
    if time.clock() - t0 > PICKLE_ARG_TIME_LIMIT:
        raise RainWarning("Pickling object {} took {} s > PICKLE_ARG_TIME_LIMIT={}. " +
                          "Consider using a blob() for the data."
                          .format(name or '<unknown>', len(d), PICKLE_ARG_TIME_LIMIT))
    return p


def _checked_cloudpickle_to_string(d, name=None):
    """Same as _changed_pickle but encodes result to base64 string"""
    return base64.b64encode(_checked_cloudpickle(d, name)).decode("ascii")


def remote(*, outputs=1,
           inputs=(),
           args=None,
           kwargs=None,
           auto_load=None,
           auto_encode=None):
    "Decorator for :py:class:`Remote`, see the documentation there."
    def make_remote(fn):
        if not inspect.isfunction(fn):
            raise RainException(
                "remote() arg {!r} is not a function".format(fn))
        return Remote(fn,
                      outputs=outputs,
                      inputs=inputs,
                      args=args,
                      kwargs=kwargs,
                      auto_load=auto_load,
                      auto_encode=auto_encode)
    return make_remote


class Remote(TaskBuilder):
    def __init__(self,
                 fn, *,
                 inputs=(),
                 args=None,
                 kwargs=None,
                 outputs=1,
                 auto_load=None,
                 auto_encode=None):
        if 'return' in fn.__annotations__:
            assert outputs == 1
            outputs = fn.__annotations__['return']
        super().__init__(inputs=(), more_inputs=Input(), outputs=outputs, more_outputs=None)
        self.fn = fn
        self.auto_encode = auto_encode
        self.auto_load = auto_load

    def __call__(self, *args, output=None, outputs=None, session=None, **kwargs):
        # TODO(gavento): Use Input()s arguments
        if session is None:
            session = get_active_session()

        # cache the code in a static blob
        fn_blob = session._static_data.get(self.fn)
        if fn_blob is None:
            d = _checked_cloudpickle(self.fn, self.fn.__name__)
            fn_blob = blob(d, self.fn.__name__, content_type="cloudpickle")
            fn_blob.keep()
            session._static_data[self.fn] = fn_blob

        inputs = [fn_blob]

        # Check the parameter compatibility for fn
        # Note that the first arg is the context
        sig = inspect.signature(self.fn)
        sig.bind(None, *args, **kwargs)

        # Pickle positional args
        pickled_args = []
        for i, argval in enumerate(args):
            # Within this session state, the DataObjects are seialized as
            # subworker.unpickle_input_object call
            code = self.fn.__code__
            if i < code.co_argcount - 1:
                name = code.co_varnames[i + 1]
            else:
                args_name = code.co_varnames[code.co_argcount +
                                             code.co_kwonlyargcount]
                name = "{}[{}]".format(args_name, i + 1 - code.co_argcount)
            with _pickle_inputs_context(name, inputs):
                d = _checked_cloudpickle_to_string(argval, name=name)
                pickled_args.append(d)

        # Pickle keyword args
        pickled_kwargs = OrderedDict()
        for name, argval in kwargs.items():
            # Within this session state, the DataObjects are seialized as
            # subworker.unpickle_input_object call
            with _pickle_inputs_context(name, inputs):
                d = _checked_cloudpickle_to_string(argval)
                pickled_kwargs[name] = d

        # create list of Output objects and DO instances
        outs, out_dos = self.create_outputs(output=output,
                                            outputs=outputs,
                                            session=session)
        for o in outs:
            if o.encode is None or o.encode == "":
                o.encode = self.auto_encode

        task_config = {
            'args': pickled_args,
            'kwargs': pickled_kwargs,
            'auto_load': self.auto_load,
            'outputs': [o.to_json() for o in outs],
        }

        return Task("py", task_config, inputs, out_dos)


''' 
def remote(outputs=1, auto_load=None, auto_encode=None):
    # TODO: (gavento) use pickle_outputs and outputs spec
    def make_remote(fn):

        def make_task(*args, **kwargs):
            session = get_active_session()
            fn_blob = session._static_data.get(fn)
            if fn_blob is None:
                d = _checked_cloudpickle(fn, fn.__name__)
                fn_blob = blob(d, fn.__name__, content_type="cloudpickle")
                fn_blob.keep()
                session._static_data[fn] = fn_blob
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
                code = fn.__code__
                if i < code.co_argcount - 1:
                    name = code.co_varnames[i + 1]
                else:
                    args_name = code.co_varnames[code.co_argcount +
                                                 code.co_kwonlyargcount]
                    name = "{}[{}]".format(args_name, i + 1 - code.co_argcount)
                with _pickle_inputs_context(name, inputs):
                    d = _checked_cloudpickle_to_string(argval, name=name)
                    pickled_args.append(d)
            # Pickle positional args
            pickled_kwargs = OrderedDict()
            for name, argval in kwargs.items():
                # Within this session state, the DataObjects are seialized as
                # InputPlaceholders
                with _pickle_inputs_context(name, inputs):
                    d = _checked_cloudpickle_to_string(argval)
                    pickled_kwargs[name] = d
            task_config = {
                'args': pickled_args,
                'kwargs': pickled_kwargs,
                'auto_load': auto_load,
                'outputs': outputs,
            }
            return Task("py", task_config, inputs, outputs)

        if not inspect.isfunction(fn):
            raise RainException(
                "remote() arg {!r} is not a function".format(fn))
        return make_task
    return make_remote
 '''