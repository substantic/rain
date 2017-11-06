

def test_listen_argument1(test_env):
    test_env.start(1, listen_addr="127.0.0.1", listen_port="33112")


def test_listen_argument2(test_env):
    test_env.start(1, listen_addr="0.0.0.0", listen_port="33112")
