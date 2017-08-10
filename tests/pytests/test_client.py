from rain.client import session
from rain import RainException

import pytest


def test_get_info(test_env):
    test_env.start(2)  # Start server with no workers
    client = test_env.client

    info = client.get_server_info()
    assert info["n_workers"] == 2


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

    with pytest.raises(RainException):
        session.get_active_session()


def test_new_session_id(test_env):
    test_env.start(0)
    client = test_env.client

    s1 = client.new_session()
    s2 = client.new_session()

    assert s1.session_id != s2.session_id

