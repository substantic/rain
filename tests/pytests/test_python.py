from rain.client import Task, remote


def test_remote_bytes_inout(test_env):
    """Pytask returning bytes"""

    @remote()
    def hello(data):
        return data.to_bytes() + b" rocks!"

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = hello("Rain")
        s.submit()
        assert b"Rain rocks!" == t1.out.output.fetch()
