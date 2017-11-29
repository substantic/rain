

class Table:

    """Combination of list/dict that enables access
    by indices, slices and string keys
    """

    def __init__(self, items, labels):
        self.items = tuple(items)
        self.labels = labels

    def __getitem__(self, key):
        if isinstance(key, str):
            return self.labels[key]
        return self.items[key]

    def __iter__(self):
        return iter(self.items)

    def __len__(self):
        return len(self.items)

    def __contains__(self, key):
        return any(key == item.label for item in self.items)

    def label_pairs(self):
        return ((self.labels.get(item), item) for item in self.items)

    def __repr__(self):
        return "<Table ({})>".format(",".join("{}:{}".format(label, value)
                                              if label else str(value)
                                              for label, value in
                                              self.label_pairs()))
