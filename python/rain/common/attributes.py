import json


def filter_empty(d):
    """Filter out pairs with non-truthy values."""
    return {k: v for k, v in d.items() if v}


class BaseSpec:
    def __init__(self, data):
        self.user = data.pop('user', {})
        self.id = data.pop('id', None)

    def _to_json(self):
        return filter_empty({
            'user': self.user,
            'id': self.id._to_json()
        })


class BaseInfo:
    def __init__(self, data):
        self.error = data.pop('error', "")
        self.debug = data.pop('debug', "")
        self.user = data.pop('user', {})

    def _to_json(self):
        return filter_empty({
            'error': self.error,
            'debug': self.debug,
            'user': self.user,
        })


class TaskSpec(BaseSpec):
    pass


class TaskInfo(BaseInfo):
    def __init__(self, data):
        # Make a copy to be modified
        data = dict(data)
        super.__init__(data)
        self.start_time = data.pop('start_time', None)
        self.duration = data.pop('duration', None)
        self.governor = data.pop('governor', None)
        if data:
            raise ValueError("Unknown {} attributes: {}".format(
                self.__class__.__name__, list(data.keys())
            ))

    def _to_json(self):
        r = super()._to_json()
        r.update(filter_empty({
            'start_time': self.start_time,
            'duration': self.duration,
            'governor': self.governor,
        }))
        return r


class DataObjectSpec(BaseSpec):
    pass


class DataObjectInfo(BaseInfo):
    pass

    def _to_json(self):
        pass



class AttributesBase:

    def __init__(self, data=None):
        """Initialize the base class, optionally from a json-like dict."""
        if self.__class__ == AttributesBase:
            raise TypeError("Do not create instances of AttributesBase, use TaskAttributes or ObjectAttributes.")
        if data is None:
            data = {}
        self.spec = self.Spec()
        self.spec.__dict__ = data.get('spec', {})
        self.info = self.Info()
        self.info.__dict__ = data.get('info', {})
        self.debug = data.get('debug', "")
        self.error = data.get('error', None)
        self.user_info = data.get('user_info', {})
        self.user_spec = data.get('user_spec', {})

    def _to_json(self):
        """Return a json-encodable object with the non-empty attributes."""
        r = {}
        if self.spec.__dict__:
            r['spec'] = self.spec.__dict__
        if self.info.__dict__:
            r['info'] = self.info.__dict__
        if self.debug:
            r['debug'] = self.debug
        if self.error is not None:
            r['error'] = self.error
        if self.user_spec:
            r['user_spec'] = self.user_spec
        if self.user_info:
            r['user_info'] = self.user_info
        return r

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, self._to_json())

    @classmethod
    def _from_capnp(cls, attributes):
        return cls({item.key: (json.loads(item.value))
                   for item in attributes.items})

    def _to_capnp(self, builder):
        data = self._to_json()
        items = builder.init("items", len(data))
        for i, item in enumerate(data.items()):
            items[i].key = item[0]
            items[i].value = json.dumps(item[1])


class TaskAttributes(AttributesBase):
    """Attributes of a Task instance."""

    class Spec:
        pass

    class Info:
        pass


class ObjectAttributes(AttributesBase):
    """Attributes of a DataObject instance."""

    class Spec:
        pass

    class Info:
        pass
