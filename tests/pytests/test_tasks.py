
from rain.client import Task
from rain.client.data import DataObject, Blob


import pytest


def test_task_construction(fake_session):
    with fake_session as session:
        t1 = Task("dummy")
        t2 = Task("dummy")

        assert t1.session == session
        assert t2.session == session
        assert t1.id != t2.id


def test_task_outputs(fake_session):
    with fake_session:
        t1 = Task("dummy",
                  outputs=(Blob("a"),
                           Blob("long_name"),
                           Blob("space inside"),
                           Blob("")))

        assert "a" in t1.outputs
        assert "space inside" in t1.outputs
        assert "XXX" not in t1.outputs

        assert isinstance(t1.outputs["a"], DataObject)
        assert isinstance(t1.outputs["space inside"], DataObject)
        assert isinstance(t1.outputs["long_name"], DataObject)

        with pytest.raises(KeyError):
            t1.outputs["XXX"]

        assert t1.outputs["a"] != t1.outputs[3]
        assert t1.outputs["a"] == t1.outputs[0]
        assert t1.outputs["a"] != t1.outputs["long_name"]
        assert t1.outputs[1] == t1.outputs["long_name"]


def test_task_keep_outputs(fake_session):
    with fake_session:
        t = Task("dummy", outputs=[Blob("a"), Blob("b"), Blob("c")])
        assert all(not t.is_kept() for t in t.outputs)
        t.keep_outputs()
        assert all(t.is_kept() for t in t.outputs)
        t.unkeep_outputs()
        assert all(not t.is_kept() for t in t.outputs)
