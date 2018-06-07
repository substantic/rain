from rain.client import tasks, blob
from rain.client import TaskException, Task, Input, Output
import pytest


class ExecutorTester:

    def __init__(self, name, path):
        self.name = name
        self.path = path
        self.tasks = {}

    def task(self, method, **kwargs):
        kwargs.setdefault("inputs", ())
        cls = self.tasks.get(method)
        if cls is None:
            class MyClass(Task):
                TASK_TYPE = self.name + "/" + method
            cls = MyClass
            self.tasks[method] = cls
        return cls(**kwargs)

    def task_hello(self, obj):
        return self.task("hello", inputs=(obj,), outputs=1)

    def task_fail(self, obj):
        return self.task("fail", inputs=(obj,), outputs=0)

    def start(self, test_env, nodes=1):
        test_env.start(nodes, executor=(self.name, self.path))

    def test_hello_mem(self, test_env):
        self.start(test_env)
        with test_env.client.new_session() as s:
            t1 = self.task_hello(blob("world"))
            t1.output.keep()
            s.submit()
            assert t1.output.fetch().get_bytes() == b"Hello world!"

    def test_hello_file(self, test_env):
        self.start(test_env)
        with test_env.client.new_session() as s:
            d1 = blob("WORLD")
            t0 = tasks.Execute("ls",
                               input_paths=[Input("d1", dataobj=d1)],
                               output_paths=[Output("d1")])
            t1 = self.task_hello(t0.output)
            t1.output.keep()
            s.submit()
            assert t1.output.fetch().get_bytes() == b"Hello WORLD!"

    def test_fail(self, test_env):
        self.start(test_env)
        with test_env.client.new_session() as s:
            t1 = self.task_fail(blob("ABCD"))
            s.submit()
            with pytest.raises(TaskException, match='ABCD'):
                t1.wait()

    def test_invalid_inputs(self, test_env):
        self.start(test_env)
        with test_env.client.new_session() as s:
            obj = blob("WORLD")
            t1 = self.task("hello", inputs=(obj, obj, obj), outputs=1)
            s.submit()
            with pytest.raises(TaskException, match='3'):
                t1.wait()

    def test_invalid_outputs(self, test_env):
        self.start(test_env)
        with test_env.client.new_session() as s:
            obj = blob("WORLD")
            t1 = self.task("hello", inputs=(obj,), outputs=3)
            s.submit()
            with pytest.raises(TaskException, match='3'):
                t1.wait()

    def test_invalid(self, test_env):
        self.start(test_env)
        with test_env.client.new_session() as s:
            t1 = self.task("this_should_not_exist", outputs=0)
            s.submit()
            with pytest.raises(TaskException, match='this_should_not_exist'):
                t1.wait()

    def test_panic(self, test_env):
        self.start(test_env)
        with test_env.client.new_session() as s:
            t1 = self.task("panic", outputs=0)
            s.submit()
            with pytest.raises(TaskException, match='panicked on purpose'):
                t1.wait()

    def test_hello_chain(self, test_env):
        self.start(test_env)
        with test_env.client.new_session() as s:
            t1 = blob("world")
            for i in range(30):
                t1 = self.task_hello(t1).output
            t1.keep()
            s.submit()
            assert t1.fetch().get_bytes() == b"Hello " * 30 + b"world" + b"!" * 30

    def test_hello_burst(self, test_env):
        self.start(test_env)
        with test_env.client.new_session() as s:
            data = blob("world")
            outputs = [self.task_hello(data).output for i in range(50)]
            for output in outputs:
                output.keep()
            s.submit()
            for output in outputs:
                assert output.fetch().get_bytes() == b"Hello world!"
