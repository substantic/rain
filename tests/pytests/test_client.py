from rain.client import rpc, session, tasks
from rain.client import RainException
from rain.client import Program

import pytest


def test_get_info(test_env):
    test_env.start(2)  # Start server with no workers
    client = test_env.client

    info = client.get_server_info()
    workers = info["workers"]
    assert len(workers) == 2
    for w in workers:
        assert w["n_tasks"] == 0
        assert w["n_objects"] == 0


def test_session_autoclose(test_env):

    test_env.start(0)

    s = test_env.client.new_session()
    assert s.active
    with s:
        assert s.active
    assert not s.active

    def helper():
        with s:
            pass
    pytest.raises(RainException, helper)


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


def test_active_session_bind_only(test_env):
    test_env.start(0)

    with pytest.raises(Exception):
        session.get_active_session()

    client = test_env.client
    s = client.new_session()

    with s.bind_only():
        assert session.get_active_session() == s

        with client.new_session().bind_only() as s2:
            assert session.get_active_session() != s
            assert session.get_active_session() == s2

        with s.bind_only():
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
    test_env.no_final_check()
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


@pytest.mark.xfail(reason="wait_some not implemented")
def test_wait_some(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.concat("a", "b")
        t2 = tasks.sleep(0.4, t1)
        s.submit()
        finished = s.wait_some((t1,), ())
        assert t1.state == rpc.common.TaskState.finished
        assert t2.state == rpc.common.TaskState.notAssigned
        assert len(finished) == 2
        assert len(finished[0]) == 1
        assert len(finished[1]) == 0
        assert finished[0][0].id == t1.id
        t2.wait()

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


def test_unkeep_finished(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.concat("a", "b")
        t1_output = t1.out.output
        t1_output.keep()
        t2 = tasks.sleep(0.3, t1)
        with pytest.raises(RainException):
            t1_output.unkeep()
        s.submit()
        t1.wait()
        assert t1_output.is_kept() is True
        t1_output.unkeep()
        assert t1_output.is_kept() is False
        t2.wait()


def test_unkeep_unfinished(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.concat("a", "b")
        t1_output = t1.out.output
        t1_output.keep()
        t2 = tasks.sleep(0.3, t1)
        with pytest.raises(RainException):
            t1_output.unkeep()
        s.submit()
        assert t1_output.is_kept() is True
        t1_output.unkeep()
        assert t1_output.is_kept() is False
        t2.wait()


def test_unkeep_failed(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        args = ("/bin/non-existing-program",)
        program = Program(args, stdout="output")
        t1 = program()
        t1_output = t1.out.output
        t1_output.keep()
        s.submit()

        import time
        time.sleep(0.6)

        with pytest.raises(RainException):
            t1_output.unkeep()
        with pytest.raises(RainException):
            t1_output.unkeep()


def test_get_state(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.concat("a", "b")
        s.submit()
        s.get_state((t1,), ())
        t1.wait()
        s.get_state((t1,), ())


def test_task_wait(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s.bind_only():
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
        t1.wait()


@pytest.mark.xfail(reason="Fetching from failed session not implemented yet")
def test_fetch_from_failed_session(test_env):
    """Setting input file for program"""
    test_env.start(1)
    args = ("/bin/non-existing-program",)
    program = Program(args, stdout="output")
    with test_env.client.new_session() as s:
        t1 = program()
        t1.out.output.keep()
        s.submit()
        with pytest.raises(RainException):
            t1.out.output.fetch()


@pytest.mark.xfail(reason="Server now do not support waiting on objects")
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
