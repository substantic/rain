
from rain.client import remote

from rain.client import blob, Program, tasks
import string
import random
import pytest

CHARS = string.ascii_letters + string.digits


@pytest.fixture
def test_rnd():
    return random.Random("Rain")


def random_string(rnd, length):
    return "".join(rnd.choice(CHARS) for i in range(length))


def run_small_gridcat(session):
    BLOB_SIZE = 5000
    BLOB_COUNT = 10

    rnd = test_rnd()

    def random_string(rnd, length):
        return "".join(rnd.choice(CHARS) for i in range(length))

    cat = Program("cat input1 input2",
                  stdout="output").input("input1").input("input2")
    md5sum = Program("md5sum", stdin="input", stdout="output")

    @remote()
    def take_first(data):
        return data.to_bytes().split()[0]

    consts = [blob(random_string(rnd, BLOB_SIZE)) for i in range(BLOB_COUNT)]
    ts = []
    for i in range(BLOB_COUNT):
        for j in range(BLOB_COUNT):
            t1 = cat(input1=consts[i], input2=consts[j])
            t2 = md5sum(input=t1)
            t3 = take_first(t2)
            ts.append(t3.out.output)
    result = md5sum(input=tasks.concat(*ts))
    result.out.output.keep()
    #  session.pending_graph().write("/home/spirali/tmp/graph.dot")
    session.submit()
    result.out.output.fetch() == b"0a9612a2e855278d336a9e1a1589478f  -\n"


def test_small_gridcat_1(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        run_small_gridcat(s)


def test_small_gridcat_2(test_env):
    test_env.start(2)
    with test_env.client.new_session() as s:
        run_small_gridcat(s)


def test_small_gridcat_4(test_env):
    test_env.start(4)
    with test_env.client.new_session() as s:
        run_small_gridcat(s)


def test_big_diamond(test_env):

    @remote(outputs=("out1", "out2"))
    def splitter(data):
        data = data.to_bytes()
        left = data[0:len(data)]
        right = data[len(data):]
        return {"out1": left, "out2": right}

    @remote()
    def upper(data):
        return data.to_bytes().upper()

    LAYERS = 6
    rnd = test_rnd()
    data = random_string(rnd, 100).lower().encode()

    test_env.start(4)
    with test_env.client.new_session() as s:
        layer = [data]
        for i in range(LAYERS):
            new_layer = []
            for l in layer:
                task = splitter(l)
                new_layer.append(task.out.out1)
                new_layer.append(task.out.out2)
            layer = new_layer
        layer = [upper(t) for t in layer]

        for i in range(LAYERS):
            new_layer = []
            for j in range(0, len(layer), 2):
                new_layer.append(tasks.concat(layer[j], layer[j+1]))
            layer = new_layer
        #  s.pending_graph().write("test.dot")
        assert len(layer) == 1
        result = layer[0]
        result.out.output.keep()
        s.submit()
        result = result.out.output.fetch()
        assert result == data.upper()


def test_separated_lines(test_env):

    @remote()
    def op(data):
        data = data.to_bytes()
        return data + data[:1]

    N_LINES = 30
    STEPS = 4

    initial_data = [chr(ord("A") + i).encode() for i in range(N_LINES)]
    streams = initial_data

    test_env.start(4)
    with test_env.client.new_session() as s:

        for i in range(STEPS):
            streams = [op(t) for t in streams]

        for t in streams:
            t.out.output.keep()

        s.submit()
        checkpoint = streams

        for i in range(STEPS):
            streams = [op(t) for t in streams]

        for t in streams:
            t.out.output.keep()

        for i in range(STEPS):
            streams = [op(t) for t in streams]

        for t in streams:
            t.out.output.keep()

        s.submit()

        results1 = [t.out.output.fetch() for t in checkpoint]
        results2 = [t.out.output.fetch() for t in streams]

        assert results1 == [d * STEPS + d for d in initial_data]
        assert results2 == [d * STEPS * 3 + d for d in initial_data]
