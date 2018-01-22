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
        assert "info" not in t1.attributes
        t1.update()
        assert t1.attributes["info"]["worker"].startswith("127.0.0.1:")
        t1.update()
        assert t1.attributes["info"]["worker"].startswith("127.0.0.1:")
        assert 300 < int(t1.attributes["info"]["duration"]) < 600
        start = t1.attributes["info"]["start"]
        # rust sends time in more precision than python parses,
        # TODO: investigate what is according RFC
        # for now, lets just trim padding
        start = start[:start.index(".") + 6]
        time.strptime(start, '%Y-%m-%dT%H:%M:%S.%f')