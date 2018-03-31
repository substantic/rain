from rain.client import blob, RainException, pickled, tasks, directory, Input, Output
import rain
import pytest
import json
import pickle
import os


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
        assert b3.content_type == 'pickle'

        b3b = pickled(obj)
        assert b3b.data == b3.data
        assert b3b.content_type == 'pickle'

        b4 = blob(obj, encode='json')
        assert json.loads(b4.data.decode()) == obj
        assert rain.common.content_type.decode_value(b4.data, "json") == obj

        txt = "asžčďďŠ"
        b5 = blob(txt, encode='text:latin2')
        assert b5.data.decode('latin2') == txt

        with pytest.raises(RainException):
            blob(123)


def test_dir_big(test_env):

    data = b"01234567890" * 1024 * 1024

    os.mkdir("dir")
    with open("dir/file1", "wb") as f:
        f.write(data)

    test_env.start(1)
    with test_env.client.new_session() as s:
        d = directory("dir")
        t = tasks.execute("cat d/file1",
                          input_paths=[Input("d", dataobj=d)],
                          stdout=True,
                          output_paths=[Output("d", content_type="dir")])
        t.keep_outputs()
        s.submit()
        assert t.outputs["stdout"].fetch().get_bytes() == data
        t.outputs["d"].fetch().write("result")
        with open("result/file1", "rb") as f:
            assert f.read() == data