
from rain.client import remote

from rain.client import blob, Program, tasks
import string


def run_small_gridcat(session):
    BLOB_SIZE = 5000
    BLOB_COUNT = 10

    import random
    rnd = random.Random("Rain")

    CHARS = string.ascii_letters + string.digits

    def random_string(length):
        return "".join(rnd.choice(CHARS) for i in range(length))

    cat = Program("cat input1 input2",
                  stdout="output").input("input1").input("input2")
    md5sum = Program("md5sum", stdin="input", stdout="output")

    @remote()
    def take_first(data):
        return data.to_bytes().split()[0]

    consts = [blob(random_string(BLOB_SIZE)) for i in range(BLOB_COUNT)]
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
