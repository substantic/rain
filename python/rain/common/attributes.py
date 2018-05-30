from .errors import RainException
from .ids import ID
from .utils import short_str


class AttributeBase:
    """Base class for Task/Object Spec/Info objects.

    _ATTRS is a dictionary `"attribute_name": (from_json, to_json, default)`.
    The convertors should raise a `TypeError` or `ValueError` on invalid value.
    `default()` is used as the default.
    """
    _ATTRS = {}

    def __init__(self):
        for n, ftj in self._ATTRS.items():
            self.__setattr__(n, ftj[2]())

    @classmethod
    def _from_json(cls, data):
        s = cls()
        for n, v in data.items():
            if n not in cls._ATTRS:
                raise AttributeError("Unknown attribute {} for {}".format(n, cls))
            fj = cls._ATTRS[n][0]
            try:
                val = fj(v)
            except (TypeError, ValueError) as e:
                raise RainException(
                    "Error loading value {:r} of type {} as an attribute {} of {}".format(
                        val, type(val), n, cls)) from e
            s.__setattr__(n, val)
        return s

    def _to_json(self):
        r = {}
        for n, ftj in self._ATTRS.items():
            val = self.__getattribute__(n)
            if val:
                try:
                    r[n] = ftj[1](val)
                except (TypeError, ValueError) as e:
                    raise RainException(
                        "Can't convert {} attribute {} from value {:r} of type {}".format(
                            self.__class__, n, val, type(val))) from e
        return r

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, short_str(self._to_json(), max_len=42))


class TaskSpec(AttributeBase):
    """Task specification data.

    Attributes:
        id (`ID`): Task ID tuple.
        task_type (`str`): The task-type identificator (`"executor/method"`).
        user (`dict` with `str` keys): Arbitrary user json-serializable attributes.
        config (json-serializable): Task-type specific configuration data.
    """
    _ATTRS = {
        "id": (ID._from_json, lambda x: x._to_json(), lambda: ID(0, 0)),
        "task_type": (str, str, str),
        "user": (dict, dict, dict),
        "config": (lambda x: x, lambda x: x, lambda: None)
        # TODO: more
    }


class TaskInfo(AttributeBase):
    """Task runtime info.ID

    Attributes:
        error (`str`): Error message. Non-empty error indicates failure. 
            NB: Empty string is NOT a failure.
        start_time (`time`): Time the task was started.
        duration (`float`): Real-time duration in seconds (milisecond precision).
        governor (`str`): The ID (address) of the governor executing this task.
        user (`dict` with `str` keys): Arbitrary json-serializable objects.
        debug (`str`): Free-form debugging log. This is the only mutable attribute,
            should be append-only.
    """
    _ATTRS = {
        "error": (str, str, str),
        "start_time": (str, str, str),  # TODO: time object
        "duration": (lambda ms: 0.001 * ms, lambda s: int(s * 1000), lambda: None),
        "governor": (str, str, str),
        "user": (dict, dict, dict),
        "debug": (str, str, str),
    }
