from rain.client import tasks, blob
from rain.client import TaskException, Task, Input, Output

import pytest


def cpp_hello(obj):
    return Task("cpptester/hello", inputs=(obj,), outputs=1)


def cpp_fail(obj):
    return Task("cpptester/fail", inputs=(obj,), outputs=0)


def cpp_invalid():
    return


def test_cpp_hello_mem(test_env):
    test_env.start(1, executor="cpptester")
    with test_env.client.new_session() as s:
        t1 = cpp_hello(blob("world"))
        t1.output.keep()
        s.submit()
        assert t1.output.fetch().get_bytes() == b"Hello world!"


def test_cpp_hello_file(test_env):
    test_env.start(1, executor="cpptester")
    with test_env.client.new_session() as s:
        d1 = blob("WORLD")
        t0 = tasks.execute("ls",
                           input_paths=[Input("d1", dataobj=d1)],
                           output_paths=[Output("d1")])
        t1 = cpp_hello(t0.output)
        t1.output.keep()
        s.submit()
        assert t1.output.fetch().get_bytes() == b"Hello WORLD!"


def test_cpp_fail(test_env):
    test_env.start(1, executor="cpptester")
    with test_env.client.new_session() as s:
        t1 = cpp_fail(blob("ABCD"))
        s.submit()
        with pytest.raises(TaskException, match='ABCD'):
            t1.wait()


def test_cpp_invalid_inputs(test_env):
    test_env.start(1, executor="cpptester")
    with test_env.client.new_session() as s:
        obj = blob("WORLD")
        t1 = Task("cpptester/hello", inputs=(obj, obj, obj), outputs=1)
        s.submit()
        with pytest.raises(TaskException, match='3'):
            t1.wait()


def test_cpp_invalid_outputs(test_env):
    test_env.start(1, executor="cpptester")
    with test_env.client.new_session() as s:
        obj = blob("WORLD")
        t1 = Task("cpptester/hello", inputs=(obj,), outputs=3)
        s.submit()
        with pytest.raises(TaskException, match='3'):
            t1.wait()


def test_cpp_invalid(test_env):
    test_env.start(1, executor="cpptester")
    with test_env.client.new_session() as s:
        t1 = Task("cpptester/this_should_not_exist", outputs=0)
        s.submit()
        with pytest.raises(TaskException, match='this_should_not_exist'):
            t1.wait()


def test_cpp_panic(test_env):
    test_env.start(1, executor="cpptester")
    with test_env.client.new_session() as s:
        t1 = Task("cpptester/panic", outputs=0)
        s.submit()
        with pytest.raises(TaskException, match='panicked on purpose'):
            t1.wait()

# TODO: Chain, task burst
