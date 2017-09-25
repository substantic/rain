
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
    with fake_session as session:
        t1 = Task("dummy",
                  outputs=(Blob("a"),
                           Blob("long_name"),
                           Blob("space inside"),
                           Blob("")))

        assert t1.has_output("a")
        assert t1.has_output("space inside")
        assert t1.has_output("")
        assert not t1.has_output("XXX")

        assert isinstance(t1.out["a"], DataObject)
        assert isinstance(t1.out[""], DataObject)
        assert isinstance(t1.out["space inside"], DataObject)
        assert isinstance(t1.out["long_name"], DataObject)

        with pytest.raises(KeyError):
            t1.out["XXX"]

        assert t1.out["a"] != t1.out[""]
        assert t1.out["a"] != t1.out["long_name"]

        assert t1.out["a"] == t1.out.a
        assert t1.out["long_name"] == t1.out.long_name
