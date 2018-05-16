from rain.client import rpc, session, tasks, blob
from rain.client import RainException, TaskException, Task
from rain.client import Program

import pytest
import time


def cpp_hello(obj):
    return Task("cpptester/hello", inputs=(obj,), outputs=1)

def test_get_info(test_env):
    test_env.start(1, subworker="cpptester")
    with test_env.client.new_session() as s:
        t1 = cpp_hello(blob("world"))
        s.submit()
        t1.wait()


