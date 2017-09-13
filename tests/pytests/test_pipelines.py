from rain.client import rpc, session, tasks
from rain import RainException

import pytest


def test_sleep1(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.sleep(0.3, "b")
        s.submit()
        test_env.assert_duration(0.29, 0.4, lambda: t1.wait())
