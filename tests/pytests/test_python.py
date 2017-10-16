from rain.client import Task, remote


def test_remote_bytes_inout(test_env):
    """Pytask taking and returning bytes"""

    @remote()
    def hello(data):
        return data.to_bytes() + b" rocks!"

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = hello("Rain")
        t1.out.output.keep()
        s.submit()
        assert b"Rain rocks!" == t1.out.output.fetch()


def test_remote_more_bytes_outputs(test_env):
    """Pytask returning more tasks"""

    @remote(outputs=("x1", "x2"))
    def test():
        return {"x1": b"One", "x2": b"Two"}

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        t1.out.x1.keep()
        t1.out.x2.keep()
        s.submit()
        assert b"One" == t1.out.x1.fetch()
        assert b"Two" == t1.out.x2.fetch()


def test_remote_exception(test_env):

    @remote()
    def test():
        raise Exception("Hello world!")

    test_env.start(1)
    with test_env.client.new_session() as s:
       t1 = test()
       s.submit()
       t1.wait()


def test_remote_exception_sleep(test_env):

    @remote()
    def test():
        import time
        time.sleep(0.2)
        raise Exception("Hello world!")

    test_env.start(1)
    with test_env.client.new_session() as s:
       t1 = test()
       s.submit()
       t1.wait()
