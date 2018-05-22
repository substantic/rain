from executor_tester import ExecutorTester


tester = ExecutorTester("cpptester")


def test_cpp_hello_mem(test_env):
    tester.test_hello_mem(test_env)


def test_cpp_hello_file(test_env):
    tester.test_hello_file(test_env)


def test_cpp_fail(test_env):
    tester.test_fail(test_env)


def test_cpp_invalid_inputs(test_env):
    tester.test_invalid_inputs(test_env)


def test_cpp_invalid_outputs(test_env):
    tester.test_invalid_outputs(test_env)


def test_cpp_invalid(test_env):
    tester.test_invalid(test_env)


def test_cpp_panic(test_env):
    tester.test_panic(test_env)


def test_cpp_hello_chain(test_env):
    tester.test_hello_chain(test_env)


def test_cpp_hello_burst(test_env):
    tester.test_hello_burst(test_env)
