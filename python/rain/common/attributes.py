import re

from .data_type import DataType
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

    def __init__(self, **kwargs):
        """
        Initialize the attributes with their default values or the
        values provided in `**kwargs`.
        """
        for n, ftj in self._ATTRS.items():
            self.__setattr__(n, ftj[2]())
        for n, v in kwargs.items():
            if n in self._ATTRS:
                self.__setattr__(n, v)
            else:
                raise TypeError("Unknown attribute name {!r}".format(n))

    @classmethod
    def _camelize(_cls, snake_name):
        """
        Convert `snake_case` to `camelCase`. A bit naive (internal only).
        """
        first, *rest = snake_name.split('_')
        return first + ''.join(word.capitalize() for word in rest)

    @classmethod
    def _snakeit(_cls, camel_name):
        """
        Convert `camelCase` to `snake_case`. A bit naive (internal only).
        """
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel_name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    @classmethod
    def _from_json(cls, data):
        s = cls()
        for n, v in data.items():
            if n not in cls._ATTRS:
                raise AttributeError("Unknown attribute {} for {}".format(n, cls))
            fj = cls._ATTRS[n][0] or (lambda x: x)
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
                    r[n] = (ftj[1] or (lambda x: x))(val)
                except (TypeError, ValueError) as e:
                    raise RainException(
                        "Can't convert {} attribute {} from value {:r} of type {}".format(
                            self.__class__, n, val, type(val))) from e
        return r

    def __repr__(self):
        return "<{} {}>".format(
            self.__class__.__name__,
            short_str(self._to_json(), max_len=42))


class TaskSpecInput(AttributeBase):
    """Task input specification.

    Attributes:
        id (`ID`): Input object ID.
        label (`str`): Optional label.
    """
    _ATTRS = {
        "id": (ID._from_json, lambda x: x._to_json(), lambda: ID(0, 0)),
        "label": (str, str, str),
    }


class TaskSpec(AttributeBase):
    """Task specification data.

    Attributes:
        id (`ID`): Task ID tuple.
        task_type (`str`): The task-type identificator (`"executor/method"`).
        config (json-serializable): Task-type specific configuration data.
        inputs (`list` of `TaskSpecInput`): Input object IDs with their labels.
        outputs (`list` of `ID`): Output object IDs.
        resources (`dict` with `str` keys): Resource specification.
        user (`dict` with `str` keys): Arbitrary user json-serializable attributes.
    """
    _ATTRS = {
        "id": (ID._from_json, lambda x: x._to_json(), lambda: None),
        "task_type": (str, str, str),
        "config": (None, None, lambda: None),
        "inputs": (
            lambda il: [TaskSpecInput._from_json(i) for i in il],
            lambda il: [i._to_json() for i in il],
            list),
        "outputs": (list, list, lambda: {}),
        "resources": (dict, dict, lambda: {}),
        "user": (dict, dict, dict),
    }


class TaskInfo(AttributeBase):
    """Task runtime info.

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
        "error": (str, str, lambda: None),
        "start_time": (str, str, str),  # TODO: to/from time object
        "duration": (float, float, lambda: None),
        "governor": (str, str, str),
        "user": (dict, dict, dict),
        "debug": (str, str, str),
    }


class ObjectSpec(AttributeBase):
    """Data object specification data.

    Attributes:
        id (`ID`): Task ID tuple.
        label (`str`): Label (role) of this output at the generating task.
        content_type (`str`): Content type name.
        data_type (`str`): Object type, "blob" or "directory".
        user (`dict` with `str` keys): Arbitrary user json-serializable attributes.
    """
    _ATTRS = {
        "id": (ID._from_json, lambda x: x._to_json(), lambda: None),
        "label": (str, str, str),
        "content_type": (str, str, str),
        "data_type": (DataType, lambda x: DataType(x).value, lambda: DataType.BLOB),
        "user": (dict, dict, dict),
    }


class ObjectInfo(AttributeBase):
    """Data object runtime info.

    Attributes:
        error (`str`): Error message. Non-empty error indicates failure.
            NB: Empty string is NOT a failure.
        size (`int`): Final size in bytes (approximate for directories).
        content_type (`str`): Content type name.
        user (`dict` with `str` keys): Arbitrary json-serializable objects.
        debug (`str`): Free-form debugging log. This is the only mutable attribute,
            should be append-only.
    """
    _ATTRS = {
        "error": (str, str, str),
        "size": (int, int, int),
        "content_type": (str, str, str),
        "user": (dict, dict, dict),
        "debug": (str, str, str),
    }
