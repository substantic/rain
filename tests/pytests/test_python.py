from rain.client import Task, remote


def test_remote_str_output(test_env):
    """Pytask returning bytes"""

    @remote()
    def hello():
        return b"Rain" + b" rocks!"

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = hello()
        s.submit()
        assert b"Rain rocks!" == t1.out.output.fetch()
