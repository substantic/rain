from rain.client import remote
import time


def test_update_attributes(test_env):

    @remote()
    def test(ctx):
        import time
        time.sleep(0.3)
        return b""

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        s.submit()
        t1.wait()
        assert t1.info is None
        t1.update()
        assert t1.info.governor.startswith("127.0.0.1:")
        t1.update()
        assert t1.info.governor.startswith("127.0.0.1:")
        assert 300 < int(t1.info.duration) < 600


def test_remote_attributes(test_env):

    @remote()
    def test(ctx):
        #ctx. # TODO
        return b""

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        s.submit()
        t1.wait()
        assert t1.info is None
        t1.update()
        assert t1.info.governor.startswith("127.0.0.1:")
        t1.update()
        assert t1.info.governor.startswith("127.0.0.1:")
        assert 300 < int(t1.info.duration) < 600
