from rain.client import remote, Program, Input, blob
from rain.client import RainException, RainWarning
import pytest
import pickle


def test_remote_bytes_inout(test_env):
    """Pytask taking and returning bytes"""

    @remote()
    def hello(ctx, data):
        return data.to_bytes() + b" rocks!"

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = hello(blob("Rain"))
        t1.output.keep()
        s.submit()
        assert b"Rain rocks!" == t1.output.fetch()


def test_remote_more_bytes_outputs(test_env):
    """Pytask returning more tasks"""

    @remote(outputs=("x1", "x2"))
    def test(ctx):
        return {"x1": b"One", "x2": b"Two"}

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        t1.outputs["x1"].keep()
        t1.outputs["x2"].keep()
        s.submit()
        assert b"One" == t1.outputs["x1"].fetch()
        assert b"Two" == t1.outputs["x2"].fetch()


def test_remote_exception(test_env):

    # TODO: Check error message
    # but "match" in pytest.raises somehow do not work??

    @remote()
    def test(ctx):
        raise Exception("Hello world!")

    test_env.start(1)

    for i in range(10):
        with test_env.client.new_session() as s:
            t1 = test()
            t1.output.keep()
            s.submit()

            with pytest.raises(RainException):
                t1.wait()
            with pytest.raises(RainException):
                t1.wait()
            with pytest.raises(RainException):
                t1.output.fetch()


def test_remote_exception_sleep(test_env):

    @remote()
    def test(ctx):
        import time
        time.sleep(0.2)
        raise Exception("Hello world!")

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        t1.output.keep()
        s.submit()
        with pytest.raises(RainException):
            t1.wait()
        with pytest.raises(RainException):
            t1.wait()
        with pytest.raises(RainException):
                t1.output.fetch()


def test_remote_exception_fetch_after_delay(test_env):
    import time

    @remote()
    def test(ctx):
        raise Exception("Hello world!")

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        t1.output.keep()
        s.submit()
        time.sleep(0.6)
        with pytest.raises(RainException):
            t1.output.fetch()
        with pytest.raises(RainException):
            t1.output.fetch()
        with pytest.raises(RainException):
            t1.wait()


def test_remote_exception_fetch_immediate(test_env):

    @remote()
    def test(ctx):
        import time
        time.sleep(0.3)
        raise Exception("Hello world!")

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        t1.output.keep()
        s.submit()
        with pytest.raises(RainException):
            t1.output.fetch()
        with pytest.raises(RainException):
            t1.output.fetch()
        with pytest.raises(RainException):
            t1.wait()


def test_python_invalid_output(test_env):

    @remote()
    def test(ctx):
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
    def test(ctx):
        return "Hello world!"

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        t1.output.keep()
        s.submit()
        assert b"Hello world!" == t1.output.fetch()


def test_py_same_subworker(test_env):

    @remote()
    def first(ctx):
        import os
        return str(os.getpid())

    @remote()
    def second(ctx, prev):
        import os
        assert prev.to_str() == str(os.getpid())
        return prev

    test_env.start(1)
    with test_env.client.new_session() as s:
        t = first()
        for i in range(30):
            t = second(t)
        t.output.keep()
        s.submit()
        assert int(t.output.fetch())


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
        t1.output.keep()
        s.submit()
        assert b"Hello world!" == t1.output.fetch()


def test_py_pass_through(test_env):
    @remote(outputs=("out1", "out2"))
    def test(ctx, data1, data2):
        return {"out1": data1, "out2": data2}

    test_env.start(1)

    cat = Program("/bin/cat input1", stdout="output", inputs=[Input("input1")])

    with test_env.client.new_session() as s:
        data = b"ABC" * 10000
        t0 = cat(input1=blob(data))
        t1 = test(t0, blob("Hello!"))
        t1.outputs["out1"].keep()
        t1.outputs["out2"].keep()
        s.submit()
        assert data == t1.outputs["out1"].fetch()
        assert b"Hello!" == t1.outputs["out2"].fetch()


def test_py_ctx_debug(test_env):
    @remote()
    def test(ctx):
        ctx.debug("First message")
        ctx.debug("Second message")
        ctx.debug("Last message")
        return b"Result"

    test_env.start(1)
    with test_env.client.new_session() as s:
        t0 = test()
        s.submit()
        t0.wait()
        t0.update()
        assert t0.attrs["debug"] == \
            "First message\nSecond message\nLast message"


def test_py_ctx_set(test_env):
    @remote()
    def test(ctx):
        ctx.attrs["string"] = "value"
        ctx.attrs["int"] = 103
        ctx.attrs["float"] = 77.12
        ctx.attrs["boolTrue"] = True
        ctx.attrs["boolFalse"] = False
        ctx.attrs["data"] = b"ABC"
        return b"Test"

    test_env.start(1)
    with test_env.client.new_session() as s:
        t0 = test()
        s.submit()
        t0.wait()
        t0.update()
        assert t0.attrs["string"] == "value"
        assert t0.attrs["int"] == 103
        assert t0.attrs["float"] == 77.12
        assert t0.attrs["boolTrue"] is True
        assert t0.attrs["boolFalse"] is False
        assert t0.attrs["data"] == b"ABC"


def test_remote_complex_args(test_env):

    @remote()
    def test(ctx, a, b, c={}, d=0, **kwargs):
        ret = (a, b.to_bytes(), c['a'].to_bytes(), c['b'][3].to_bytes(),
               d, kwargs['e'](4).to_bytes())
        return pickle.dumps(ret)

    @remote()
    def test2(ctx, a, *args):
        pass

    test_env.start(1)
    with test_env.client.new_session() as s:

        bs = [blob(str(i)) for i in range(5)]
        t0 = test([True], bs[0], {"a": bs[1], "b": bs},
                  d=42, e=lambda x: bs[x])
        t0.output.keep()
        s.submit()
        d = t0.output.fetch()
        assert pickle.loads(d) == ([True], b'0', b'1', b'3', 42, b'4')

        # TODO: Test labeling with LabeledList
        # t2 = test2(*bs)
        # assert t2.inputs[1].label == 'a{0}'
        # assert t2.inputs[2][0] == 'args[0]{0}'
        # assert t2.inputs[3][0] == 'args[1]{0}'


def test_remote_arg_signature(fake_session):

    @remote()
    def test(ctx, a, c={}, *args, d): pass

    with fake_session:
        with pytest.raises(TypeError, match="required argument: 'a'"):
            test()
        with pytest.raises(TypeError, match="required argument: 'd'"):
            test(0, e=0)
        with pytest.raises(TypeError, match="required argument: 'a'"):
            test(d=0)
        test(0, d=True)


def test_remote_large_args(fake_session):

    "Reject >1M direct argument to py task, accept <1K argument"
    @remote()
    def test(ctx, a): pass

    with fake_session:
        with pytest.raises(RainWarning,
                           match='Pickled object a length'):
            test("X" * 1024 * 1024)
        test("X" * 1024)
