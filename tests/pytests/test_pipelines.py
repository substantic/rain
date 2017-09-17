from rain.client import rpc, session, tasks
from rain import RainException

import pytest


def test_sleep1(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.sleep(0.3, "abc123456")
        s.submit()
        test_env.assert_duration(0.29, 0.4, lambda: t1.wait())
        result = test_env.assert_max_duration(0.05, lambda: t1.out.output.fetch())
        assert result == b"abc123456"


def test_sleep3_last(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.sleep(0.2, "b")
        t2 = tasks.sleep(0.3, t1)
        t3 = tasks.sleep(0.2, t2)
        s.submit()
        test_env.assert_duration(0.29, 0.4, lambda: t3.wait())
