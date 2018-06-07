from rain.client import remote


def test_update_attributes(test_env):

    @remote()
    def test(ctx):
        import time
        time.sleep(0.3)
        ctx.info.user["test123"] = ["A", 1, 2]
        return b""

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        s.submit()
        t1.wait()
        assert t1.info is None
        t1.update()
        assert t1.info.governor.startswith("127.0.0.1:")
        assert 0.3 < t1.info.duration < 0.6
        assert t1.info.user["test123"] == ["A", 1, 2]
        d = t1.info.duration
        t1.update()
        assert t1.info.governor.startswith("127.0.0.1:")
        t1.info.duration == d
