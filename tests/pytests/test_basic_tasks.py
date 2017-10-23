from rain.client import tasks
from rain.client import RainException

import pytest


def test_sleep1(test_env):
    """Sleep followed by wait"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.sleep(0.3, "abc123456")
        t1.out.output.keep()
        s.submit()
        test_env.assert_duration(0.2, 0.4, lambda: t1.wait())
        result = test_env.assert_max_duration(0.1,
                                              lambda: t1.out.output.fetch())
        assert result == b"abc123456"


def test_sleep2(test_env):
    """Sleep followed by fetch (without explicit wait)"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.sleep(0.3, "abc123456")
        t1.out.output.keep()
        s.submit()
        result = test_env.assert_duration(0.028, 0.45,
                                          lambda: t1.out.output.fetch())
        assert result == b"abc123456"


def test_concat1(test_env):
    """Merge several short blobs"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.concat("Hello ", "", "", "world", "!", "")
        t1.out.output.keep()
        s.submit()
        assert t1.out.output.fetch() == b"Hello world!"


def test_concat2(test_env):
    """Merge empty list of blobs"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.concat()
        t1.out.output.keep()
        s.submit()
        assert t1.out.output.fetch() == b""


def test_concat3(test_env):
    """Merge empty large blobs"""
    test_env.start(1)
    a = b"a123" * 1000000
    b = b"b43" * 2500000
    c = b"c" * 1000
    with test_env.client.new_session() as s:
        t1 = tasks.concat(a, c, b, c, a)
        t1.out.output.keep()
        s.submit()
        assert t1.out.output.fetch() == a + c + b + c + a


def test_chain_concat(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.concat("a", "b")
        t2 = tasks.concat(t1, "c")
        t3 = tasks.concat(t2, "d")
        t4 = tasks.concat(t3, "e")
        t5 = tasks.concat(t4, "f")
        t5.out.output.keep()
        s.submit()
        assert t5.out.output.fetch() == b"abcdef"


def test_sleep3_last(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.sleep(0.2, "b")
        t2 = tasks.sleep(0.2, t1)
        t3 = tasks.sleep(0.2, t2)
        s.submit()
        test_env.assert_duration(0.4, 0.8, lambda: t3.wait())
