from rain.client import rpc, session, tasks
from rain import RainException

import pytest


def test_program_sleep_1(test_env):
    """Sleep followed by wait"""
    test_env.start(1)
    program = tasks.Program("sleep 1")
    with test_env.client.new_session() as s:
        t1 = program()
        s.submit()
        test_env.assert_duration(0.99, 1.1, lambda: t1.wait())
