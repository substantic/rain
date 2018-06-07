
# NOTE: Actual executor tests are place in test_XXX_tasklib for each tasklib
# This is small test for executor machinery itself

from rain.client import TaskException, Task
import pytest


def test_executor_no_registration(test_env):

    class InvalidTask(Task):
        TASK_TYPE = "xxx/abc"

    test_env.start(1, executor=("xxx", "/bin/ls"))
    with test_env.client.new_session() as s:
        t1 = InvalidTask(inputs=(), outputs=0)
        s.submit()
        with pytest.raises(TaskException, match="stdout"):
          t1.wait()
