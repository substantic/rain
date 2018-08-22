from rain.client import rpc, session, tasks, blob
from rain.client import RainException, TaskException
from rain.client import Program

import pytest
import time


def test_get_info(test_env):
    test_env.start(2)
    client = test_env.client

    info = client.get_server_info()
    governors = info["governors"]
    assert len(governors) == 2
    for w in governors:
        assert w["tasks"] == []
        assert w["objects"] == []


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


def test_session_default(test_env):
    test_env.start(0)

    client = test_env.client
    s = client.new_session(default=True)
    assert session.get_active_session() == s

    s2 = client.new_session()
    with s2:
        assert session.get_active_session() == s2

    assert session.get_active_session() == s
    s.close()

    with pytest.raises(Exception):
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
        t1 = tasks.Concat((blob("a"), blob("b")))
        t2 = tasks.Sleep(t1, 1)
        assert s.task_count == 2
        assert s.dataobj_count == 4  # "a", "b", "ab", "ab"
        s.submit()
        assert s.task_count == 0
        assert s.dataobj_count == 0
        assert t1.state == rpc.TaskState.NotAssigned
        assert t2.state == rpc.TaskState.NotAssigned


@pytest.mark.xfail(reason="wait_some not implemented")
def test_wait_some(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.Concat(("a", "b"))
        t2 = tasks.Sleep(t1, 0.4)
        s.submit()
        finished = s.wait_some((t1,), ())
        assert t1.state == rpc.TaskState.Finished
        assert t2.state == rpc.TaskState.NotAssigned
        assert len(finished) == 2
        assert len(finished[0]) == 1
        assert len(finished[1]) == 0
        assert finished[0][0].id == t1.id
        t2.wait()


def test_wait_all(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.Concat((blob("a"), blob("b")))
        t2 = tasks.Sleep(t1, 0.5)
        s.submit()
        test_env.assert_duration(0.35, 0.65, lambda: s.wait_all())
        assert t1.state == rpc.TaskState.Finished
        assert t2.state == rpc.TaskState.Finished
        test_env.assert_max_duration(0.1, lambda: t2.wait())


def test_late_wait_all_failed(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        args = ("/bin/non-existing-program",)
        program = Program(args, stdout="output")
        t1 = program()
        t1_output = t1.output
        t1_output.keep()
        s.submit()
        time.sleep(0.3)
        with pytest.raises(TaskException):
            s.wait_all()


def test_early_wait_all_failed_(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t0 = tasks.Sleep(blob("test"), 0.4)
        args = ("/bin/non-existing-program")
        program = Program(args, stdout="output", stdin="input")
        t1 = program(input=t0)
        t1_output = t1.output
        t1_output.keep()
        s.submit()
        with pytest.raises(TaskException):
            s.wait_all()


def test_wait_all_empty(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        s.submit()
        test_env.assert_max_duration(0.1, lambda: s.wait_all())


def test_wait_unsubmitted_task(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.Concat(())
        with pytest.raises(RainException):
            t1.wait()


def test_fetch_unsubmitted_task(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.Concat(())
        t1.keep_outputs()
        with pytest.raises(RainException):
            t1.fetch_outputs()


def test_unkeep_finished(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.Concat((blob("a"), blob("b")))
        t1_output = t1.output
        t1_output.keep()
        t2 = tasks.Sleep(t1, 0.3)
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
        t1 = tasks.Concat((blob("a"), blob("b")))
        t1_output = t1.output
        t1_output.keep()
        t2 = tasks.Sleep(t1, 0.3)
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
        t1_output = t1.output
        t1_output.keep()
        s.submit()

        time.sleep(0.6)

        with pytest.raises(TaskException):
            t1_output.unkeep()
        with pytest.raises(TaskException):
            t1_output.unkeep()


def test_update(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.Concat((blob("a"), blob("b")))
        s.submit()
        s.update((t1,))
        t1.wait()
        s.update((t1,))


def test_task_wait(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s.bind_only():
        t1 = tasks.Concat((blob("a"), blob("b")))
    assert t1.state is None
    s.submit()
    assert t1.state == rpc.TaskState.NotAssigned
    t1.wait()
    assert t1.state == rpc.TaskState.Finished


def test_fetch_removed_object_fails(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.Sleep(blob("abc123456"), 0.1)
        s.submit()
        with pytest.raises(RainException):
            t1.output.fetch()
        t1.wait()


def test_fetch_from_failed_session_immediate(test_env):
    """Setting input file for program"""
    test_env.start(1)
    args = ("/bin/non-existing-program",)
    program = Program(args, stdout="output")
    with test_env.client.new_session() as s:
        t1 = program()
        t1.output.keep()
        s.submit()
        with pytest.raises(TaskException):
            t1.output.fetch()
        with pytest.raises(TaskException):
            t1.output.fetch()


def test_update_from_failed_session(test_env):
    """Setting input file for program"""
    test_env.start(1)
    args = ("/bin/non-existing-program",)
    program = Program(args, stdout="output")
    with test_env.client.new_session() as s:
        t1 = program()
        t1.output.keep()
        s.submit()
        time.sleep(0.6)
        with pytest.raises(TaskException):
            t1.update()
        with pytest.raises(TaskException):
            t1.output.update()


@pytest.mark.xfail(reason="Server now do not support waiting on objects")
def test_dataobj_wait(test_env):
    test_env.start(1)
    client = test_env.client
    s = client.new_session()
    with s:
        t1 = tasks.Concat((blob("a"), blob("b")))
        o1 = t1.output
        assert t1.state is None
        s.submit()
        assert o1.state == rpc.DataObjectState.Unfinished
        o1.wait()
        assert o1.state == rpc.DataObjectState.Finished


def test_fetch_outputs(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t0 = tasks.Execute("ls /", stdout=True)
        t1 = tasks.Execute(("split", "-d", "-n", "l/2", t0),
                           output_paths=["x00", "x01"])
        t2 = tasks.Execute("ls /", stdout=True)

        t2.keep_outputs()
        t1.keep_outputs()
        s.submit()
        a = t2.output.fetch()
        b = t1.fetch_outputs()

        assert len(a.get_bytes()) > 4
        assert b[0].get_bytes() + b[1].get_bytes() == a.get_bytes()
