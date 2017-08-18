
from rain.client.task import Task
from rain.client.data import DataObject
from rain import RainException

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
        t1 = Task("dummy", outputs=("a", "long_name", "space inside", ""))

        assert t1.has_output("a")
        assert t1.has_output("space inside")
        assert t1.has_output("")
        assert not t1.has_output("XXX")

        assert isinstance(t1["a"], DataObject)
        assert isinstance(t1[""], DataObject)
        assert isinstance(t1["space inside"], DataObject)
        assert isinstance(t1["long_name"], DataObject)

        with pytest.raises(RainException):
            t1["XXX"]

        assert t1["a"] != t1[""]
        assert t1["a"] != t1["long_name"]

        assert t1["a"] == t1.out_a
        assert t1["long_name"] == t1.out_long_name
        assert t1[""] == t1.out
