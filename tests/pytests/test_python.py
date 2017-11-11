from rain.client import remote, RainException, Program
import pytest


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

    # TODO: Check error message
    # but "match" in pytest.raises somehow do not work??

    @remote()
    def test():
        raise Exception("Hello world!")

    test_env.start(1)

    for i in range(10):
        with test_env.client.new_session() as s:
            t1 = test()
            t1.out.output.keep()
            s.submit()

            with pytest.raises(RainException):
                t1.wait()
            with pytest.raises(RainException):
                t1.wait()
            with pytest.raises(RainException):
                t1.out.output.fetch()


def test_remote_exception_sleep(test_env):

    @remote()
    def test():
        import time
        time.sleep(0.2)
        raise Exception("Hello world!")

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        t1.out.output.keep()
        s.submit()
        with pytest.raises(RainException):
            t1.wait()
        with pytest.raises(RainException):
            t1.wait()
        with pytest.raises(RainException):
                t1.out.output.fetch()


def test_remote_exception_fetch_after_delay(test_env):
    import time

    @remote()
    def test():
        raise Exception("Hello world!")

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        t1.out.output.keep()
        s.submit()
        time.sleep(0.6)
        with pytest.raises(RainException):
            t1.out.output.fetch()
        with pytest.raises(RainException):
            t1.out.output.fetch()
        with pytest.raises(RainException):
            t1.wait()


def test_remote_exception_fetch_immediate(test_env):

    @remote()
    def test():
        import time
        time.sleep(0.3)
        raise Exception("Hello world!")

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        t1.out.output.keep()
        s.submit()
        with pytest.raises(RainException):
            t1.out.output.fetch()
        with pytest.raises(RainException):
            t1.out.output.fetch()
        with pytest.raises(RainException):
            t1.wait()


def test_python_invalid_output(test_env):

    @remote()
    def test():
        class X():
            pass
        return X()

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        s.submit()
        with pytest.raises(RainException):
            t1.wait()


def test_string_output(test_env):

    @remote()
    def test():
        return "Hello world!"

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        t1.out.output.keep()
        s.submit()
        assert b"Hello world!" == t1.out.output.fetch()


def test_py_file_output(test_env):
    @remote()
    def test(ctx):
        import os
        assert not os.listdir(".")
        with open("test_file", "w") as f:
            f.write("Hello world!")
        f = ctx.stage_file("test_file")
        assert not os.listdir(".")
        return f

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        t1.out.output.keep()
        s.submit()
        assert b"Hello world!" == t1.out.output.fetch()


def test_py_file_pass_through(test_env):
    @remote()
    def test(data):
        return data

    test_env.start(1)

    cat = Program("/bin/cat", stdout="output").arg_path("input1")

    with test_env.client.new_session() as s:
        data = b"ABC" * 10000
        t0 = cat(input1=data)
        t1 = test(t0)
        t1.out.output.keep()
        s.submit()
        assert b"Hello world!" == t1.out.output.fetch()
