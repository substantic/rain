from rain.client import rpc, session, tasks
from rain import RainException

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
    test_env.start(0)
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


def test_wait(test_env):
    test_env.start(0)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.concat("a", "b")
    s.submit()
    assert t1.state == rpc.common.TaskState.notAssigned
    s.wait((t1,), ())
    assert t1.state == rpc.common.TaskState.finished


def test_wait_some(test_env):
    test_env.start(0)
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


def test_wait_all(test_env):
    test_env.start(0)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.concat("a", "b")
    s.submit()
    s.wait_all()
    assert t1.state == rpc.common.TaskState.finished


def test_remove(test_env):
    test_env.start(0)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.concat("a", "b")
        t1_output = list(t1.outputs.values())[0]
        t1_output.keep()
        t2 = tasks.sleep(1, t1)
    with pytest.raises(RainException):
        t1_output.remove()
    s.submit()
    assert t1_output.is_kept() is True
    t1_output.remove()
    assert t1_output.is_kept() is False


def test_get_state(test_env):
    test_env.start(0)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.concat("a", "b")
    s.submit()
    s.get_state((t1,), ())
