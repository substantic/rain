from rain.client import remote, Program, Input, Output, blob
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


def test_python_termination(test_env):

    @remote()
    def test1(ctx):
        import time
        time.sleep(5)

    @remote()
    def test2(ctx):
        return b"ab"

    test_env.start(1)
    import time

    with test_env.client.new_session() as s:
        test1()
        s.submit()
        time.sleep(0.5)

    with test_env.client.new_session() as s:
        t1 = test2()
        t1.keep_outputs()
        s.submit()
        r = test_env.assert_max_duration(0.30, lambda: t1.output.fetch())
        assert b"ab" == r


@pytest.mark.xfail(reason="not functional now")
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
        assert t0.attributes["debug"] == \
            "First message\nSecond message\nLast message"


def test_py_loadsave(test_env):

    @remote()
    def test(ctx, a):
        assert a.load() == [10, 20]
        o = ctx.blob(["a", 1], encode="json")
        assert o.load() == ["a", 1]

        return ctx.blob(["a", 1], encode="json")

    test_env.start(1)
    with test_env.client.new_session() as s:
        a = blob(b"[10, 20]", content_type="json")
        b = test(a)
        b.output.keep()
        s.submit()
        s.wait_all()
        assert b.output.fetch() == b'["a", 1]'
        # TODO(gavento): implement dynamic data object types
        # assert b.output.fetch().load() == ["a", 1]


def test_py_ctx_set_attributes(test_env):
    @remote()
    def test(ctx, a):
        assert a.attributes["first"] == "first value"
        assert a.attributes["second"] == {"integer": 12, "list": [1, 2, 3]}
        assert ctx.attributes["in_string"] == "value"
        assert ctx.attributes["in_complex"] == {"abc": 1200, "xyz": 321.12}

        ctx.attributes["string"] = "value"
        ctx.attributes["int"] = 103
        ctx.attributes["float"] = 77.12
        ctx.attributes["boolTrue"] = True
        ctx.attributes["boolFalse"] = False
        ctx.attributes["dict"] = {"abc": 1, "xyz": "zzz"}

        a.attributes["new"] = ["a", 10, "b"]
        return a

    test_env.start(1)
    with test_env.client.new_session() as s:
        d0 = blob("data")
        d0.attributes["first"] = "first value"
        d0.attributes["second"] = {"integer": 12, "list": [1, 2, 3]}
        t0 = test(d0)
        t0.attributes["in_string"] = "value"
        t0.attributes["in_complex"] = {"abc": 1200, "xyz": 321.12}
        s.submit()
        t0.wait()
        t0.update()
        assert t0.attributes["string"] == "value"
        assert t0.attributes["int"] == 103
        assert t0.attributes["float"] == 77.12
        assert t0.attributes["boolTrue"] is True
        assert t0.attributes["boolFalse"] is False
        assert t0.attributes["dict"] == {"abc": 1, "xyz": "zzz"}

        o = t0.output
        o.update()
        assert o.attributes["first"] == "first value"
        assert o.attributes["second"] == {"integer": 12, "list": [1, 2, 3]}
        assert o.attributes["new"] == ["a", 10, "b"]


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


def test_output_detailed_specs(test_env):
    "Tests specifying content types for outputs and dynamic content types."

    obj = {1: 2, 3: [4, 5]}
    obj2 = [1.0, 2.0, True]

    @remote(outputs=[Output(encode='pickle', label='test_pickle', size_hint=0.1),
                     Output(content_type='text:latin2'),
                     "out_c", "out_d"])
    def test1(ctx):
        return (obj, b'\xe1\xb9\xef\xeb', pickle.dumps(obj2),
                ctx.blob("[42.0]", content_type='json'))

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test1(outputs=[Output(),
                            Output(),
                            Output(content_type='pickle'),
                            Output()])
        t1.keep_outputs()
        s.submit()
        (a, b, c, d) = t1.fetch_outputs()
        assert a.load() == obj
        assert b.load() == 'ášďë'
        assert b == b'\xe1\xb9\xef\xeb'
        assert c.load() == obj2
        assert t1.outputs.out_c.fetch().load == obj2
        assert d == b"[42.0]"
        with pytest.raises():
            assert d.content_type == 'json'
            assert d.load() == [42.0]


def test_output_specs_num(test_env):
    @remote(outputs=3)
    def test1(ctx):
        return (b'HW', b'\xe1\xb9\xef\xeb', pickle.dumps([2.0, 3.0]))

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test1(outputs=[Output(),
                            Output(content_type='text:latin2'),
                            Output(content_type='pickle')])
        t1.keep_outputs()
        s.submit()
        (a, b, c) = t1.fetch_outputs()
        assert b.load() == 'ášďë'
        assert c.load() == [2.0, 3.0]


def test_auto_load_and_encode(test_env):

    @remote(auto_load=True, auto_encode='pickle')
    def test_add(ctx, a, b):
        return {'msg': a[0] + b + a[1]}

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test_add()
        t1.keep_outputs(["H", "d"], "ello worl")
        s.submit()
        assert t1.output.fetch().load()['msg'] == "Hello world"
