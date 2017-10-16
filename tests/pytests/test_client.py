from rain.client import rpc, session, tasks
from rain.client import RainException

import pytest


def test_get_info(test_env):
    test_env.start(2)  # Start server with no workers
    client = test_env.client

    info = client.get_server_info()
    assert info["n_workers"] == 2


def test_active_session(test_env):
    test_env.start(0)

    with pytest.raises(Exception):
        session.get_active_session()

    client = test_env.client
    s = client.new_session()

    with s:
        assert session.get_active_session() == s

        with client.new_session() as s2:
            assert session.get_active_session() != s
            assert session.get_active_session() == s2

        with s:
            assert session.get_active_session() == s

        assert session.get_active_session() == s

    with pytest.raises(RainException):
        session.get_active_session()


def test_new_session_id(test_env):
    test_env.start(0)
    client = test_env.client

    s1 = client.new_session()
    s2 = client.new_session()

    assert s1.session_id != s2.session_id


def test_submit(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.concat("a", "b")
        t2 = tasks.sleep(1, t1)
    assert s.task_count == 2
    assert s.dataobj_count == 4  # "a", "b", "ab", "ab"
    s.submit()
    assert s.task_count == 0
    assert s.dataobj_count == 0
    assert t1.state == rpc.common.TaskState.notAssigned
    assert t2.state == rpc.common.TaskState.notAssigned


def test_wait_some(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.concat("a", "b")
        t2 = tasks.sleep(1, t1)
    s.submit()
    finished = s.wait_some((t1,), ())
    assert t1.state == rpc.common.TaskState.finished
    assert t2.state == rpc.common.TaskState.notAssigned
    assert len(finished) == 2
    assert len(finished[0]) == 1
    assert len(finished[1]) == 0
    assert finished[0][0].id == t1.id

@pytest.mark.xfail(reason="server wait does not support the sepcial 'all' ID")
def test_wait_all(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.concat("a", "b")
    s.submit()
    s.wait_all()
    assert t1.state == rpc.common.TaskState.finished


def test_remove(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.concat("a", "b")
        t1_output = t1.out.output
        t1_output.keep()
        t2 = tasks.sleep(1, t1)
    with pytest.raises(RainException):
        t1_output.remove()
    s.submit()
    assert t1_output.is_kept() is True
    t1_output.remove()
    assert t1_output.is_kept() is False


def test_get_state(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.concat("a", "b")
    s.submit()
    s.get_state((t1,), ())


def test_task_wait(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.concat("a", "b")
    assert t1.state is None
    s.submit()
    assert t1.state == rpc.common.TaskState.notAssigned
    t1.wait()
    assert t1.state == rpc.common.TaskState.finished


def test_fetch_removed_object_fails(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.sleep(0.01, "abc123456")
        s.submit()
        with pytest.raises(RainException):
            t1.out.output.fetch()


def test_dataobj_wait(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.concat("a", "b")
        o1 = t1.out.output
    assert t1.state is None
    s.submit()
    assert o1.state == rpc.common.DataObjectState.unfinished
    o1.wait()
    assert o1.state == rpc.common.DataObjectState.finished
