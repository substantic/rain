import os

from executor_tester import ExecutorTester


PYTEST_DIR = os.path.dirname(__file__)
ROOT = os.path.dirname(os.path.dirname(PYTEST_DIR))
RUST_TASK_TESTER = os.environ.get("RUST_TASK_TESTER",
                                  os.path.join(ROOT, "target", "debug", "rain_task_test"))
tester = ExecutorTester("rusttester", RUST_TASK_TESTER)


def test_rust_hello_mem(test_env):
    tester.test_hello_mem(test_env)


def test_rust_hello_file(test_env):
    tester.test_hello_file(test_env)


def test_rust_fail(test_env):
    tester.test_fail(test_env)


def test_rust_invalid_inputs(test_env):
    tester.test_invalid_inputs(test_env)


def test_rust_invalid_outputs(test_env):
    tester.test_invalid_outputs(test_env)


def test_rust_invalid(test_env):
    tester.test_invalid(test_env)


def test_rust_panic(test_env):
    tester.test_panic(test_env)


def test_rust_meta(test_env):
    tester.test_meta(test_env)


def test_rust_hello_chain(test_env):
    tester.test_hello_chain(test_env)


def test_rust_hello_burst(test_env):
    tester.test_hello_burst(test_env)
