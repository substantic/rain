from rain.client import remote


def test_update_additionals(test_env):

    @remote()
    def test(ctx):
        return b""

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        s.submit()
        t1.wait()
        assert "worker" not in t1.attrs
        t1.update()
        assert t1.attrs["worker"].startswith("127.0.0.1:")
        t1.update()
        assert t1.attrs["worker"].startswith("127.0.0.1:")
