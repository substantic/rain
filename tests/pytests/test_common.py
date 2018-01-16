from rain.common import LabeledList
import pytest


def test_labeled_list():
    l = LabeledList()  # noqa
    assert len(l) == 0
    l._check()
    l.append(1, label='a')
    l.append(2, label='b')
    l._check()
    assert l['a'] == 1
    assert l['b'] == 2
    assert 'a' in l
    assert 'c' not in l

    # Try duplicate labels
    with pytest.raises(KeyError):
        l.append(3, label='a')
    l._check()
    with pytest.raises(KeyError):
        l.set(1, 42, label='a')
    l._check()

    # Setting the same label should work
    l.set(0, 43, label='a')
    l._check()
    assert len(l) == 2
    assert l[0] == 43
    assert l[1] == 2
    l.set_label(0, 'a')
    assert l['a'] == 43
    l.set_label(0, 'aa')
    with pytest.raises(KeyError):
        assert l['a'] == 43

    # Data without label
    l.append(3)
    l._check()
    assert l.get_label(2) is None

    # Test popping, deleting and inserting
    assert l.pop(1) == 2
    l._check()
    l[1:1] = [42, 42]
    assert l.data == [43, 42, 42, 3]
    assert l.get_label(1) is None
    l._check()

    # list constructor
    l2 = LabeledList([2, 3, 4])
    assert l2[1] == 3
    assert l2.get_label(0) is None

    # "zip" constructor
    l3 = LabeledList([2, 3, 4], labels=['a', 'b', 'c'])
    assert l3['b'] == 3
    assert l3[2] == 4

    # pairs constructor
    l4 = LabeledList(pairs=[('a', 2), ('b', 3), ('c', 4)])
    assert l3['b'] == 3
    assert l3[2] == 4
    assert len(l3) == 3

    assert l3 == l4
    assert l3 != [2, 3, 4]

    # copy constructor
    assert l3 == LabeledList(l3)
