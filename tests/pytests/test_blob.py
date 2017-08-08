from rain import blob, RainException
import pytest


def test_blob_construction(test_env):
    test_env.start(0)  # Start server with no workers
    client = test_env.client

    with client.new_session() as session:
        b1 = blob("abc")
        assert b1.session == session

        b2 = blob(b"xyz")
        assert b1.session == session
        assert b1.id != b2.id

        with pytest.raises(RainException):
            blob(123)
