from rain.client import remote, Program, Input, Output, blob, pickled
from rain.client import RainException, RainWarning
from rain.common import DataInstance
import pytest
import pickle


def test_remote_bytes_inout(test_env):
    """Pytask taking and returning bytes"""

    @remote()
    def hello(ctx, data):
        return data.get_bytes() + b" rocks!"

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = hello(blob("Rain"))
        t1.output.keep()
        s.submit()
        assert b"Rain rocks!" == t1.output.fetch().get_bytes()


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
        assert b"One" == t1.outputs["x1"].fetch().get_bytes()
        assert b"Two" == t1.outputs["x2"].fetch().get_bytes()


def test_python_cache(test_env):

    @remote()
    def f1(ctx):
        return "f1:" + str(id(ctx.function))

    @remote()
    def f2(ctx):
        return "f2:" + str(id(ctx.function))

    test_env.start(1)
    with test_env.client.new_session() as s:
        r1 = [f1() for i in range(10)]
        r2 = [f2() for i in range(10)]
        for r in r1 + r2:
            r.output.keep()
        s.submit()
        rs1 = list(set(r.output.fetch().get_bytes() for r in r1))
        rs2 = list(set(r.output.fetch().get_bytes() for r in r2))
        assert len(rs1) == 1
        assert rs1[0].startswith(b"f1:")
        assert len(rs2) == 1
        assert rs2[0].startswith(b"f2:")


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

            with pytest.raises(RainException, match='Hello'):
                t1.wait()
            with pytest.raises(RainException, match='Hello'):
                t1.wait()
            with pytest.raises(RainException, match='Hello'):
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
        with pytest.raises(RainException, match='Hello'):
            t1.wait()
        with pytest.raises(RainException, match='Hello'):
            t1.wait()
        with pytest.raises(RainException, match='Hello'):
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
        with pytest.raises(RainException, match='Hello'):
            t1.output.fetch()
        with pytest.raises(RainException, match='Hello'):
            t1.output.fetch()
        with pytest.raises(RainException, match='Hello'):
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
        with pytest.raises(RainException, match='Hello'):
            t1.output.fetch()
        with pytest.raises(RainException, match='Hello'):
            t1.output.fetch()
        with pytest.raises(RainException, match='Hello'):
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
        assert b"Hello world!" == t1.output.fetch().get_bytes()


def test_py_same_subworker(test_env):

    @remote()
    def first(ctx):
        import os
        return str(os.getpid())

    @remote()
    def second(ctx, prev):
        import os
        assert prev.get_bytes().decode() == str(os.getpid())
        return prev

    test_env.start(1)
    with test_env.client.new_session() as s:
        t = first()
        for i in range(30):
            t = second(t)
        t.output.keep()
        s.submit()
        assert int(t.output.fetch().get_bytes())


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
        assert b"Hello world!" == t1.output.fetch().get_bytes()


def test_py_pass_through(test_env):
    @remote(outputs=("out1", "out2"))
    def test(ctx, data1, data2):
        return {"out1": data1, "out2": data2}

    test_env.start(1)

    cat = Program("/bin/cat input1", stdout="output", input_files=[Input("input1")])

    with test_env.client.new_session() as s:
        data = b"ABC" * 10000
        t0 = cat(input1=blob(data))
        t1 = test(t0, blob("Hello!"))
        t1.outputs["out1"].keep()
        t1.outputs["out2"].keep()
        s.submit()
        assert data == t1.outputs["out1"].fetch().get_bytes()
        assert b"Hello!" == t1.outputs["out2"].fetch().get_bytes()


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
        assert b"ab" == r.get_bytes()


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
        assert b.output.fetch().get_bytes() == b'["a", 1]'
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
        ret = (a, b.get_bytes(), c['a'].get_bytes(), c['b'][3].get_bytes(),
               d, kwargs['e'](4).get_bytes())
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
        d = t0.output.fetch().get_bytes()
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

    @remote()
    def test1(ctx) -> (Output(encode='pickle', label='test_pickle', size_hint=0.1),
                       Output(content_type='text:latin2'),
                       "out_c",
                       "out_d"):
        return (obj, b'\xe1\xb9\xef\xeb', pickle.dumps(obj2),
                ctx.blob(b"[42.0]", content_type='json'))

    @remote(outputs=[Output(encode='pickle', label='test_pickle', size_hint=0.1),
                     Output(content_type='text:latin2'),
                     "out_c", "out_d"])
    def test2(ctx):
        return (obj, b'\xe1\xb9\xef\xeb', pickle.dumps(obj2),
                ctx.blob(b"[42.0]", content_type='json'))

    test_env.start(1)
    for test in (test1, test2):
        with test_env.client.new_session() as s:
            t = test(outputs=[None,
                              None,
                              Output(content_type='pickle'),
                              "foo"])
            t.keep_outputs()
            s.submit()
            (a, b, c, d) = t.fetch_outputs()
            print(t.outputs)
            assert t.outputs['foo'].label == "foo"
            assert a.load() == obj
            assert b.load() == 'ášďë'
            assert b.get_bytes() == b'\xe1\xb9\xef\xeb'
            assert c.load() == obj2
            assert t.outputs['out_c'].fetch().load() == obj2
            assert d.get_bytes() == b"[42.0]"
            # Dynamic content type
            assert d.content_type == 'json'
            assert d.load() == [42.0]


def test_input_detailed_specs(test_env):
    "Tests specifying content types for inputs and dynamic content types."

    obj1 = {'A': 2, 'B': [4, 5]}
    obj2 = [1.0, 2.0, True]
    obj4 = ["a", "b"]
    obj5 = {"object": 5}

    def test_gen(ctx, in1, in2, in3, in4, in5, *args, ina="bar", **kwargs):
        assert isinstance(in1, DataInstance)
        assert in1.load() == obj1
        assert in2 == obj2
        assert in3 == (42.0, "foo")
        assert in4.load() == obj4
        assert in5 == obj5
        assert len(args) == 3
        for i in args:
            assert i.content_type == "text:latin2"
            assert i.load() == "ňů"
        assert ina == "barbar"
        assert kwargs['kwA'] == ["A"]
        assert kwargs['kwB'] == ["B"]
        assert kwargs['kwC'] == ["C"]

    @remote(inputs={'in1': Input(content_type='json'),
                    'in2': Input(content_type='pickle', load=True),
                    'in3': Input(load=True),  # expects input tuple (pickle(42.0), "foo")
                    # 'in4' only static type 'json'
                    # 'in5' has no type and no dataobject input, only python objects
                    'args': Input(content_type='text', load=False),
                    'ina': Input(load=True, content_type='cbor'),
                    'kwargs': Input(load=True),  # dynamic types, different
                    },
            outputs=0)
    def test1(ctx, in1, in2, in3, in4, in5, *args, ina="bar", **kwargs):
        test_gen(ctx, in1, in2, in3, in4, in5, *args, ina=ina, **kwargs)

    @remote()
    def test2(ctx,
              in1: Input(content_type='json'),
              in2: Input(content_type='pickle', load=True),
              in3: Input(load=True),  # expects input tuple (pickle(42.0), "foo")
              in4,  # static type 'json'
              in5,  # No type and no input, only python objects,
              *args: Input(content_type='text', load=False),
              ina: Input(load=True, content_type='cbor') ="bar",  # for 'ina'
              **kwargs: Input(load=True)  # dynamic types, different
              ) -> 0:
        test_gen(ctx, in1, in2, in3, in4, in5, *args, ina=ina, **kwargs)

    @remote()
    def copied(ctx, obj):
        "simply copy the blob, but does not provide static type info"
        return obj

    test_env.start(1)
    for test in (test1, test2):
        with test_env.client.new_session() as s:
            t1 = test(
                copied(blob(obj1, encode='json')),
                blob(pickle.dumps(obj2)),
                (pickled(42.0), "foo"),
                blob(obj4, encode='json'),
                obj5,
                blob("ňů", encode="text:latin2"),
                blob("ňů", encode="text:latin2"),
                blob("ňů", encode="text:latin2"),
                ina=blob("barbar", encode='cbor'),
                kwA=pickled(["A"]),
                kwB=blob(["B"], encode="json"),
                kwC=blob(["C"], encode="cbor"))
            s.submit()
            t1.wait()


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
        t1 = test_add(["H", "d"], "ello worl")
        t1.keep_outputs()
        s.submit()
        assert t1.output.fetch().load()['msg'] == "Hello world"


def test_python_cpus(test_env):

    @remote(auto_load=True, auto_encode='pickle', cpus=2)
    def sleep(ctx):
        import time
        time.sleep(0.5)

    test_env.start(1, n_cpus=2)
    with test_env.client.new_session() as s:
        sleep()
        sleep()
        s.submit()
        test_env.assert_duration(0.9, 1.5, lambda: s.wait_all())


def test_debug_message(test_env):

    @remote()
    def remote_fn(ctx):
        a = 11
        ctx.debug("This is first message")
        ctx.debug("This is second message and variable a = {}", a)
        return b""

    test_env.start(1)
    with test_env.client.new_session() as s:
        t = remote_fn()
        s.submit()
        t.wait()
        t.update()
        assert t.attributes["debug"] == \
            "This is first message\nThis is second message and variable a = 11"
