from rain.client import rpc, session, tasks, blob
from rain.client import RainException, TaskException, Task
from rain.client import Program

import pytest
import time


def hello(obj):
    return Task("rusttester/hello", inputs=(obj,), outputs=1)


def test_rustsw_hello(test_env):
    test_env.start(1, subworker="rusttester")
    with test_env.client.new_session() as s:
        t1 = hello(blob("world"))
        t1.keep_outputs()
        s.submit()
        assert t1.output.fetch().get_bytes() == b"Hello world!"


