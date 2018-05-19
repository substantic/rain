from rain.client import tasks, blob

import time


def test_cpu_resources1(test_env):
    """2x 1cpu tasks on 1 cpu governor"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        tasks.sleep(1.0, blob("first"))
        tasks.sleep(1.0, blob("second"))
        s.submit()
        test_env.assert_duration(1.9, 2.1, lambda: s.wait_all())


def test_cpu_resources2(test_env):
    """2x 1cpu tasks on 2 cpu governor"""
    test_env.start(1, n_cpus=2)
    with test_env.client.new_session() as s:
        tasks.sleep(1.0, blob("first"))
        tasks.sleep(1.0, blob("second"))
        s.submit()
        test_env.assert_duration(0.9, 1.1, lambda: s.wait_all())


def test_cpu_resources3(test_env):
    """1cpu + 2cpu tasks on 2 cpu governor"""
    test_env.start(1, n_cpus=2)
    with test_env.client.new_session() as s:
        tasks.sleep(1.0, blob("first"))
        tasks.sleep(1.0, blob("second"), cpus=2)
        s.submit()
        test_env.assert_duration(1.9, 2.1, lambda: s.wait_all())


def test_cpu_resources4(test_env):
    """1cpu + 2cpu tasks on 3 cpu governor"""
    test_env.start(1, n_cpus=3)
    with test_env.client.new_session() as s:
        tasks.sleep(1.0, blob("first"))
        tasks.sleep(1.0, blob("second"), cpus=2)
        s.submit()
        test_env.assert_duration(0.9, 1.1, lambda: s.wait_all())


def test_number_of_tasks_and_objects(test_env):
    """Sleep followed by wait"""
    test_env.start(1, delete_list_timeout=0)
    with test_env.client.new_session() as s:
        t1 = tasks.sleep(0.4, blob("abc123456"))
        t1.output.keep()
        s.submit()
        time.sleep(0.2)

        info = test_env.client.get_server_info()
        governors = info["governors"]
        assert len(governors) == 1
        assert governors[0]["tasks"] == [(1, 12)]
        assert sorted(governors[0]["objects"]) == [(1, 10), (1, 11)]

        t1.wait()

        # Timeout is expected as big as necessary to cleanup
        # Governor caches
        time.sleep(2)

        info = test_env.client.get_server_info()
        governors = info["governors"]
        assert len(governors) == 1
        assert governors[0]["tasks"] == []
        assert governors[0]["objects"] == [(1, 11)]

        t1.output.unkeep()

        # Timeout is expected as big as necessary to cleanup
        # Governor caches
        time.sleep(4)

        info = test_env.client.get_server_info()
        governors = info["governors"]
        assert len(governors) == 1
        assert governors[0]["tasks"] == []
        assert governors[0]["objects"] == []
