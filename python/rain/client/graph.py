class Arc(object):

    def __init__(self, node, data):
        self.node = node
        self.data = data


class Node(object):

    color = None
    fillcolor = None
    label = ""
    shape = "circle"
    fontcolor = None

    def __init__(self, key):
        self.key = key
        self.arcs = []

    def add_arc(self, node, data=None):
        self.arcs.append(Arc(node, data))

    def arc_by_data(self, data):
        for arc in self.arcs:
            if arc.data == data:
                return arc
        return None

    def merge_arcs(self, merge_fn):
        if len(self.arcs) < 2:
            return
        node_to_arcs = {}
        for arc in self.arcs[:]:
            a = node_to_arcs.get(arc.node)
            if a is None:
                node_to_arcs[arc.node] = arc
            else:
                self.arcs.remove(arc)
                a.data = merge_fn(a.data, arc.data)

    def __repr__(self):
        return "<Node {}>".format(self.key)


class Graph(object):

    def __init__(self):
        self.nodes = {}

    @property
    def size(self):
        return len(self.nodes)

    def has_node(self, key):
        return key in self.nodes

    def node_check(self, key):
        node = self.nodes.get(key)
        if node is not None:
            return (node, True)
        node = Node(key)
        self.nodes[key] = node
        return (node, False)

    def node(self, key):
        node = self.nodes.get(key)
        if node is not None:
            return node
        node = Node(key)
        self.nodes[key] = node
        return node

    def show(self):
        run_xdot(self.make_dot("G"))

    def write(self, filename):
        dot = self.make_dot("G")
        with open(filename, "w") as f:
            f.write(dot)

    def make_dot(self, name):
        stream = ["digraph " + name + " {\n"]
        for node in self.nodes.values():
            extra = ""
            if node.color is not None:
                extra += " color=\"{}\"".format(node.color)
            if node.fillcolor is not None:
                extra += " style=filled fillcolor=\"{}\"" \
                         .format(node.fillcolor)
            stream.append("v{} [label=\"{}\" shape=\"{}\"{}]\n".format(
                id(node), node.label, node.shape, extra))
            for arc in node.arcs:
                stream.append("v{} -> v{} [label=\"{}\"]\n".format(
                    id(node), id(arc.node), str(arc.data)))
        stream.append("}\n")
        return "".join(stream)

    def merge_arcs(self, merge_fn):
        for node in self.nodes.values():
            node.merge_arcs(merge_fn)


def run_xdot(dot):
    import subprocess
    import tempfile
    with tempfile.NamedTemporaryFile() as f:
        f.write(dot)
        f.flush()
        subprocess.call(("xdot", f.name))
