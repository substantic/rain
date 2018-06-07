from rain.client import tasks, blob

import time


def test_cpu_resources1(test_env):
    """2x 1cpu tasks on 1 cpu governor"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        tasks.Sleep(blob("first"), 1.0)
        tasks.Sleep(blob("second"), 1.0)
        s.submit()
        test_env.assert_duration(1.9, 2.1, lambda: s.wait_all())


def test_cpu_resources2(test_env):
    """2x 1cpu tasks on 2 cpu governor"""
    test_env.start(1, n_cpus=2)
    with test_env.client.new_session() as s:
        tasks.Sleep(blob("first"), 1.0)
        tasks.Sleep(blob("second"), 1.0)
        s.submit()
        test_env.assert_duration(0.9, 1.1, lambda: s.wait_all())


def test_cpu_resources3(test_env):
    """1cpu + 2cpu tasks on 2 cpu governor"""
    test_env.start(1, n_cpus=2)
    with test_env.client.new_session() as s:
        tasks.Sleep(blob("first"), 1.0)
        tasks.Sleep(blob("second"), 1.0, cpus=2)
        s.submit()
        test_env.assert_duration(1.9, 2.1, lambda: s.wait_all())


def test_cpu_resources4(test_env):
    """1cpu + 2cpu tasks on 3 cpu governor"""
    test_env.start(1, n_cpus=3)
    with test_env.client.new_session() as s:
        tasks.Sleep(blob("first"), 1.0)
        tasks.Sleep(blob("second"), 1.0, cpus=2)
        s.submit()
        test_env.assert_duration(0.9, 1.1, lambda: s.wait_all())


def test_number_of_tasks_and_objects(test_env):
    """Sleep followed by wait"""
    test_env.start(1, delete_list_timeout=0)
    with test_env.client.new_session() as s:
        o1 = blob("abc123456")
        t1 = tasks.Sleep(o1, 0.4)
        t1.output.keep()
        s.submit()
        time.sleep(0.2)

        info = test_env.client.get_server_info()
        governors = info["governors"]
        assert len(governors) == 1
        assert governors[0]["tasks"] == [t1.spec.id]
        assert sorted(governors[0]["objects"]) == [o1.spec.id, t1.output.id]

        t1.wait()

        # Timeout is expected as big as necessary to cleanup
        # Governor caches
        time.sleep(2)

        info = test_env.client.get_server_info()
        governors = info["governors"]
        assert len(governors) == 1
        assert governors[0]["tasks"] == []
        assert governors[0]["objects"] == [t1.output.id]

        t1.output.unkeep()

        # Timeout is expected as big as necessary to cleanup
        # Governor caches
        time.sleep(4)

        info = test_env.client.get_server_info()
        governors = info["governors"]
        assert len(governors) == 1
        assert governors[0]["tasks"] == []
        assert governors[0]["objects"] == []
