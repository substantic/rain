from rain.client import blob, RainException, pickled
import rain
import pytest
import json
import pickle


def test_blob_construction(fake_session):
    with fake_session as session:
        b1 = blob("abc")
        assert b1.session == session

        b2 = blob(b"xyz")
        assert b1.session == session
        assert b1.id != b2.id

        obj = [1, {'a': [4, 5]}]
        b3 = blob(obj, encode='pickle')
        assert pickle.loads(b3.data) == obj

        b3b = pickled(obj)
        assert b3.data == b3b.data
        assert b3.content_type == 'pickle'

        b4 = blob(obj, encode='json')
        assert json.loads(b4.data.decode()) == obj
        assert rain.common.content_type.decode_value(b4.data, "json") == obj

        txt = "asžčďďŠ"
        b5 = blob(txt, encode='text:latin2')
        assert b5.data.decode('latin2') == txt

        with pytest.raises(RainException):
            blob(123)
