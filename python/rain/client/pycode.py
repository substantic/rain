import base64
import contextlib
import inspect
import time
from collections import OrderedDict

import cloudpickle

from ..common import RainException, RainWarning
from .data import blob
from .input import Input
from .output import OutputSpec
from .session import get_active_session
from .task import Task

PICKLE_ARG_SIZE_LIMIT = 256 * 1024
PICKLE_ARG_TIME_LIMIT = 1.0


# Base name of current argument and growing list of input data objects
# while Py task arguments are pickled.
# `[arg_base_name, counter, inputs_list, input_prototype]`
_global_pickle_inputs = None


@contextlib.contextmanager
def _pickle_inputs_context(name, inputs, input_prototype):
    """Context manager to store current argument name and growing input
    objects list while Py task arguments are unpickled. Internal, not
    thread safe, not reentrant."""
    global _global_pickle_inputs
    assert _global_pickle_inputs is None
    _global_pickle_inputs = [name, 0, inputs, input_prototype]
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
        raise RainWarning("Pickled object {} length {} > PICKLE_ARG_SIZE_LIMIT={}. "
                          "Consider using a blob() for the data."
                          .format(name or '<unknown>', len(d), PICKLE_ARG_SIZE_LIMIT))
    if time.clock() - t0 > PICKLE_ARG_TIME_LIMIT:
        raise RainWarning("Pickling object {} took {} s > PICKLE_ARG_TIME_LIMIT={}. "
                          "Consider using a blob() for the data."
                          .format(name or '<unknown>', len(d), PICKLE_ARG_TIME_LIMIT))
    return p


def _checked_cloudpickle_to_string(d, name=None):
    """Same as _changed_pickle but encodes result to base64 string"""
    return base64.b64encode(_checked_cloudpickle(d, name)).decode("ascii")


def remote(*,
           outputs=None,
           inputs=(),
           auto_load=None,
           auto_encode=None,
           cpus=1):
    "Decorator for :py:class:`Remote`, see the documentation there."
    def make_remote(fn):
        if not inspect.isfunction(fn):
            raise RainException(
                "remote() arg {!r} is not a function".format(fn))
        return Remote(fn,
                      outputs=outputs,
                      inputs=inputs,
                      auto_load=auto_load,
                      auto_encode=auto_encode,
                      cpus=cpus)
    return make_remote


class Remote:
    # The function to run remotely
    fn = None
    # OutputSpec for output data objects
    outputs = None
    # Dict of named argument Input specs, including args and kwargs
    inputs = None

    def __init__(self,
                 fn, *,
                 inputs=None,
                 outputs=None,
                 auto_load=False,
                 auto_encode=None,
                 cpus=1):
        self.fn = fn
        code = self.fn.__code__
        self.cpus = cpus

        if 'return' in fn.__annotations__:
            assert outputs is None
            outputs = fn.__annotations__['return']
        elif outputs is None:
            outputs = 1
        self.outputs = OutputSpec(outputs=outputs)
        for o in self.outputs.outputs:
            if o.encode is None:
                o.encode = auto_encode

        self.inputs = {}
        for name in code.co_varnames:
            if name in inputs:
                assert name not in self.fn.__annotations__
                inp = inputs[name]
            elif name in self.fn.__annotations__:
                inp = self.fn.__annotations__[name]
            else:
                inp = Input(label=name)
            assert isinstance(inp, Input)
            if inp.load is None:
                inp.load = auto_load
            self.inputs[name] = inp

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

        input_objs = [fn_blob]

        # Check the parameter compatibility for fn
        # Note that the first arg is the context
        sig = inspect.signature(self.fn)
        sig.bind(None, *args, **kwargs)
        code = self.fn.__code__

        # Pickle positional args
        pickled_args = []
        for i, argval in enumerate(args):
            if i < code.co_argcount - 1:
                name = code.co_varnames[i + 1]
                input_proto = self.inputs[name]
            else:
                args_name = code.co_varnames[code.co_argcount +
                                             code.co_kwonlyargcount]
                name = "{}[{}]".format(args_name, i + 1 - code.co_argcount)
                input_proto = self.inputs[args_name]
            # Within this session state, the DataObjects are seialized as
            # executor.unpickle_input_object call
            assert isinstance(input_proto, Input)
            with _pickle_inputs_context(name, input_objs, input_proto):
                d = _checked_cloudpickle_to_string(argval, name=name)
                pickled_args.append(d)

        # Pickle keyword args
        pickled_kwargs = OrderedDict()
        for name, argval in kwargs.items():
            input_proto = self.inputs[code.co_varnames[-1]]
            # Within this session state, the DataObjects are seialized as
            # executor.unpickle_input_object call
            with _pickle_inputs_context(name, input_objs, input_proto):
                d = _checked_cloudpickle_to_string(argval, name=name)
                pickled_kwargs[name] = d

        # create list of Output objects and DO instances
        output_objs = self.outputs.instantiate(
            output=output, outputs=outputs, session=session)

        task_config = {
            'args': pickled_args,
            'kwargs': pickled_kwargs,
            'encode_outputs': self.outputs.encode,
        }

        return Task(input_objs, output_objs, task_type="py/", config=task_config, cpus=self.cpus)
