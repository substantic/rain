from rain.client import tasks, RainException, blob
import pytest


def test_sleep1(test_env):
    """Sleep followed by wait"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.sleep(0.3, blob("abc123456"))
        t1.output.keep()
        s.submit()
        test_env.assert_duration(0.2, 0.4, lambda: t1.wait())
        result = test_env.assert_max_duration(0.2,
                                              lambda: t1.output.fetch())
        assert result == b"abc123456"


def test_sleep2(test_env):
    """Sleep followed by fetch (without explicit wait)"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.sleep(0.3, blob("abc123456"))
        t1.output.keep()
        s.submit()
        result = test_env.assert_duration(0.028, 0.45,
                                          lambda: t1.output.fetch())
        assert result == b"abc123456"


def test_concat1(test_env):
    """Merge several short blobs"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.concat([blob(x)
                           for x in ("Hello ", "", "", "world", "!", "")])
        t1.output.keep()
        s.submit()
        assert t1.output.fetch() == b"Hello world!"


def test_concat2(test_env):
    """Merge empty list of blobs"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.concat(())
        t1.output.keep()
        s.submit()
        assert t1.output.fetch() == b""


def test_concat3(test_env):
    """Merge empty large blobs"""
    test_env.start(1)
    a = b"a123" * 1000000
    b = b"b43" * 2500000
    c = b"c" * 1000
    with test_env.client.new_session() as s:
        t1 = tasks.concat((blob(a), blob(c), blob(b), blob(c), blob(a)))
        t1.output.keep()
        s.submit()
        assert t1.output.fetch() == a + c + b + c + a


def test_chain_concat(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.concat((blob("a"), blob("b")))
        t2 = tasks.concat((t1, blob("c")))
        t3 = tasks.concat((t2, blob("d")))
        t4 = tasks.concat((t3, blob("e")))
        t5 = tasks.concat((t4, blob("f")))
        t5.output.keep()
        s.submit()
        assert t5.output.fetch() == b"abcdef"


def test_sleep3_last(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.sleep(0.2, blob("b"))
        t2 = tasks.sleep(0.2, t1)
        t3 = tasks.sleep(0.2, t2)
        s.submit()
        test_env.assert_duration(0.4, 0.8, lambda: t3.wait())


def test_task_open_not_absolute(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.open("not/absolute/path")
        s.submit()
        pytest.raises(RainException, lambda: t1.wait())


def test_task_open_not_exists(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.open("/not/exists")
        s.submit()
        pytest.raises(RainException, lambda: t1.wait())


def test_task_open_ok(test_env):
    import os.path
    path = os.path.abspath(__file__)
    with open(path, "rb") as f:
        content = f.read()
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.open(path)
        t1.output.keep()
        s.submit()
        assert t1.output.fetch() == content


def test_task_export(test_env):
    import os.path
    test1 = os.path.join(test_env.work_dir, "TEST1")
    test2 = os.path.join(test_env.work_dir, "TEST2")
    test_env.start(1)
    with test_env.client.new_session() as s:
        a = blob("Hello ")
        b = blob("World!")
        tasks.export(tasks.concat((a, b)), test1)
        tasks.export(tasks.execute("ls /", stdout="output"), test2)
        s.submit()
        s.wait_all()
        with open(test1) as f:
            assert f.read() == "Hello World!"
        with open(test2) as f:
            assert "bin" in f.read()
