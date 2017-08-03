from rain.client import session
import pytest


def test_get_info(test_env):
    test_env.start(0)  # Start server with no workers
    client = test_env.client

    info = client.get_server_info()
    assert info["n_workers"] == 0


def test_active_session(test_env):
    test_env.start(0)

    with pytest.raises(Exception):
        session.get_active_session()

    client = test_env.client
    s = client.new_session()

    with s:
        assert session.get_active_session() == s

        with client.new_session() as s2:
            assert session.get_active_session() != s
            assert session.get_active_session() == s2

        with s:
            assert session.get_active_session() == s

        assert session.get_active_session() == s

    with pytest.raises(Exception):
        session.get_active_session()
