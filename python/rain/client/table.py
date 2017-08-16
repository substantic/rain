

class Table:

    """Wrapper over dict that allows attribute like acces to keys

    >>> table = Table({"a": 10, "nice name": 11})
    >>> table["a"]
    10
    >>> table["nice name"]
    11
    >>> table.a
    10
    """

    def __init__(self, data):
        if isinstance(data, tuple):
            self.data = dict(enumerate(data))
        elif isinstance(data, dict):
            self.data = data
            for key in data:
                if key.isidentifier() and key[0] != "_":
                    setattr(self, key, data[key])
        else:
            raise Exception("Invalid type for data")

    def __getitem__(self, key):
        return self.data[key]

    def __iter__(self):
        return iter(self.data.items())

    def __len__(self):
        return len(self.data)

    def __contains__(self, key):
        return key in self.data
