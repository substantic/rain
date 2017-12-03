
class LabeledList:
    def __init__(self, items=None, labels=None, pairs=None):
        # List of any items
        self.data = []
        # List of same size as self with the labels or None
        self.labels = []
        # Dictionary label -> position in self.items
        self._index = {}

        if items is not None:
            assert pairs is None
            if labels is None:
                for val in items:
                    self.append(val)
            else:
                for val, label in zip(items, labels):
                    self.append(val, label=label)
        elif pairs is not None:
            for key, val in pairs:
                self.append(val, label=key)

    def append(self, val, label=None):
        if isinstance(label, int) or isinstance(label, slice):
            raise TypeError("{} labels may not be integers or slices"
                            .format(self.__class__.__name__))
        if label is not None:
            if label in self.labels:
                raise KeyError("Label {!r} apready present.".format(label))
            self._index[label] = len(self)
        self.data.append(val)
        self.labels.append(label)

    def pop(self):
        "Remove and return last item from the list (without the label)"
        if self.labels[-1] is not None:
            self._index.pop(self.labels[-1])
        self.labels.pop()
        return self.data.pop()

    def items(self):
        """Return iterator over (label, value) pairs.
        Do not modify the list while active."""
        return zip(self.labels, self.data)

    def __getitem__(self, key):
        "Get item by index (type int) or label. Supports slices."
        if isinstance(key, int) or isinstance(key, slice):
            return self.data[key]
        return self.data[self._index[key]]

    def __setitem__(self, key, val):
        "Sets i-th item to value, resetting any label."
        self.data[key] = val
        label = self.labels[key]
        if label is not None:
            self._index.pop(label)
            self.labels[key] = None

    def set(self, idx, val, label=None):
        if label in self._index and self._index[label] != idx:
            raise KeyError("Label {!r} apready present.".format(label))
        self[idx] = val
        if label is not None:
            self.labels[idx] = label
            self._index[label] = idx

    def get_label(self, idx):
        "Return label for given index."
        return self.labels[idx]

    def __len__(self):
        return len(self.data)

    def _check(self):
        "Assert check internal consistency. Slow."
        assert len(self.data) == len(self.labels)
        for label, i in self._index.items():
            assert self.labels[i] == label
        for i, label in enumerate(self.labels):
            if label is not None:
                assert self._index[label] == i

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.data == other.data and
                self.labels == other.labels)

    def __contains__(self, key):
        return key in self._index

    def __repr__(self):
        cont = ", ".join("{}: {}".format(label, value)
                         if label else str(value)
                         for label, value in self.items())
        return "<{} ({})>".format(self.__class__.__name__, cont)
