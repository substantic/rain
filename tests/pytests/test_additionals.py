from rain.client import blob, RainException, remote
import pytest


def test_update_additionals(test_env):

    @remote()
    def test():
        return b""

    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = test()
        s.submit()
        t1.wait()
        with pytest.raises(RainException):
            t1.get("non-existing-key")
        with pytest.raises(RainException):
            t1.get("worker")
        t1.update()
        assert t1.get("worker").startswith("127.0.0.1:")
        t1.update()
        assert t1.get("worker").startswith("127.0.0.1:")
