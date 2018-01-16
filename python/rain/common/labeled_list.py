import collections


class LabeledList(collections.MutableSequence):
    """
    List data structure with additional optional unique labels for items.
    Supports all list operations except `sort` (in general
    `collections.MutableSequence`).

    Indexing `l[x]` accepts either an integer, slice or a label.
    Modifying the sequence using `l[x]=42` clears the label.
    Use `l.set(x, 42, label='answer')` or `l.set_label(x, 'answer')`.

    Labels may be any hashable objects except `None` (which represents no
    label) or `int` or `slice` (which are used for array indexing).
    The labels must be unique.
    """

    def __init__(self, items=None, labels=None, pairs=None):
        # List of any items
        self.data = []
        # List of same size as self with the labels or None
        self.labels = []
        # Dictionary label -> position in self.items
        self._index = {}

        if isinstance(items, LabeledList):
            for l, i in items.items():
                self.append(i, label=l)
        elif items is not None:
            assert pairs is None
            if labels is None:
                for val in items:
                    self.append(val)
            else:
                assert isinstance(labels, collections.Sequence)
                for val, label in zip(items, labels):
                    self.append(val, label=label)
        elif pairs is not None:
            for key, val in pairs:
                self.append(val, label=key)

    def __delitem__(self, idx):
        self.data.__delitem__(idx)
        self.labels.__delitem__(idx)
        self._reindex()

    def insert(self, idx, val, label=None):
        self.data.insert(idx, val)
        self.labels.insert(idx, label)
        self._reindex()

    def append(self, val, label=None):
        if isinstance(label, int) or isinstance(label, slice):
            raise TypeError("{} labels may not be integers or slices"
                            .format(self.__class__.__name__))
        if label is not None:
            if label in self._index:
                raise KeyError("Label {!r} apready present.".format(label))
            self._index[label] = len(self)
        self.data.append(val)
        self.labels.append(label)

    def items(self):
        """Return iterator over (label, value) pairs.
        Do not modify the list while active."""
        return zip(self.labels, self.data)

    def __getitem__(self, key):
        "Get item by index (type int) or label. Supports slices."
        if isinstance(key, int) or isinstance(key, slice):
            return self.data[key]
        return self.data[self._index[key]]

    def __setitem__(self, idx, val):
        "Sets i-th item to value, resetting any label."
        if isinstance(idx, int):
            self.data[idx] = val
            label = self.labels[idx]
            if label is not None:
                del self._index[label]
            self.labels[idx] = None
        elif isinstance(idx, slice):
            copy = list(val)
            self.data[idx] = copy
            self.labels[idx] = (None, ) * len(copy)
            self._reindex()

    def set(self, idx, val, label=None):
        "Assign to the given index, always setting its label to `label`."
        if label in self._index and self._index[label] != idx:
            raise KeyError("Label {!r} apready present.".format(label))
        self[idx] = val
        if label is not None:
            self.labels[idx] = label
            self._index[label] = idx

    def get_label(self, idx):
        "Return the label for given index."
        return self.labels[idx]

    def set_label(self, idx, label):
        "Set label for given index."
        if label in self._index and self._index[label] != idx:
            raise KeyError("Label {!r} apready present.".format(label))
        if self.labels[idx] is not None:
            del self._index[self.labels[idx]]
        self.labels[idx] = label
        self._index[label] = idx

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

    def _reindex(self):
        "Recompute `self._index` in time O(n)."
        self._index = {}
        for idx, label in enumerate(self.labels):
            if label is not None:
                if label in self._index:
                    raise KeyError("Label {!r} apready present.".format(label))
                self._index[label] = idx

    def __eq__(self, other):
        "Equality operator, compares both data and labels."
        return (isinstance(other, self.__class__) and
                self.data == other.data and
                self.labels == other.labels)

    def __contains__(self, key):
        "Membership test for labels."
        return key in self._index

    def __repr__(self):
        cont = ", ".join("{}: {}".format(label, value)
                         if label else str(value)
                         for label, value in self.items())
        return "<{} ({})>".format(self.__class__.__name__, cont)
