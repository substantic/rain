from rain.client import tasks
from rain.client import RainException

import time
import pytest


def test_number_of_tasks_and_objects(test_env):
    """Sleep followed by wait"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.sleep(0.4, "abc123456")
        t1.out.output.keep()
        s.submit()
        time.sleep(0.2)

        info = test_env.client.get_server_info()
        workers = info["workers"]
        assert len(workers) == 1
        assert workers[0]["n_tasks"] == 1
        assert workers[0]["n_objects"] == 2

        t1.wait()

        info = test_env.client.get_server_info()
        workers = info["workers"]
        assert len(workers) == 1
        assert workers[0]["n_tasks"] == 0
        assert workers[0]["n_objects"] == 1

        t1.out.output.unkeep()
        time.sleep(1.15)

        info = test_env.client.get_server_info()
        workers = info["workers"]
        assert len(workers) == 1
        assert workers[0]["n_tasks"] == 0
        assert workers[0]["n_objects"] == 0
