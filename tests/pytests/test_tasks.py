
from rain.client import Task
from rain.client.data import DataObject
from rain.client.output import Output

import pytest


class Dummy(Task):

    TASK_TYPE = "dummy"


def test_task_construction(fake_session):
    with fake_session as session:
        t1 = Dummy(inputs=(), outputs=())
        t2 = Dummy(inputs=(), outputs=())

        assert t1._session == session
        assert t2._session == session
        assert t1.id != t2.id


def test_task_outputs(fake_session):
    with fake_session:
        t1 = Dummy(inputs=(), outputs=(Output("a", size_hint=1.0, content_type="text"),
                           Output("long_name"),
                           Output("space inside"),
                           Output("")))

        assert "a" in t1.outputs
        assert "space inside" in t1.outputs
        assert "XXX" not in t1.outputs

        assert isinstance(t1.outputs["a"], DataObject)
        assert isinstance(t1.outputs["space inside"], DataObject)
        assert isinstance(t1.outputs["long_name"], DataObject)
        assert t1.outputs['a'].content_type == "text"
        assert t1.outputs['long_name'].content_type is None

        with pytest.raises(KeyError):
            t1.outputs["XXX"]

        assert t1.outputs["a"] != t1.outputs[3]
        assert t1.outputs["a"] == t1.outputs[0]
        assert t1.outputs["a"] != t1.outputs["long_name"]
        assert t1.outputs[1] == t1.outputs["long_name"]


def test_task_keep_outputs(fake_session):
    with fake_session:
        t = Dummy(inputs=(), outputs=[
            Output("a"), Output("b"), Output("c")])
        assert all(not t.is_kept() for t in t.outputs)
        t.keep_outputs()
        assert all(t.is_kept() for t in t.outputs)
        t.unkeep_outputs()
        assert all(not t.is_kept() for t in t.outputs)
