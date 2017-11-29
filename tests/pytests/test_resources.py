from rain.client import tasks, cpus

import time


def test_cpu_resources1(test_env):
    """2x 1cpu tasks on 1 cpu worker"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        tasks.sleep(1.0, "first")
        tasks.sleep(1.0, "second")
        s.submit()
        test_env.assert_duration(1.9, 2.1, lambda: s.wait_all())


def test_cpu_resources2(test_env):
    """2x 1cpu tasks on 2 cpu worker"""
    test_env.start(1, n_cpus=2)
    with test_env.client.new_session() as s:
        tasks.sleep(1.0, "first")
        tasks.sleep(1.0, "second")
        s.submit()
        test_env.assert_duration(0.9, 1.1, lambda: s.wait_all())


def test_cpu_resources3(test_env):
    """1cpu + 2cpu tasks on 2 cpu worker"""
    test_env.start(1, n_cpus=2)
    with test_env.client.new_session() as s:
        tasks.sleep(1.0, "first")
        t1 = tasks.sleep(1.0, "second")
        t1.resources = cpus(2)
        s.submit()
        test_env.assert_duration(1.9, 2.1, lambda: s.wait_all())


def test_cpu_resources4(test_env):
    """1cpu + 2cpu tasks on 3 cpu worker"""
    test_env.start(1, n_cpus=3)
    with test_env.client.new_session() as s:
        tasks.sleep(1.0, "first")
        t1 = tasks.sleep(1.0, "second")
        t1.resources = cpus(2)
        s.submit()
        test_env.assert_duration(0.9, 1.1, lambda: s.wait_all())


def test_number_of_tasks_and_objects(test_env):
    """Sleep followed by wait"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.sleep(0.4, "abc123456")
        t1.output.keep()
        s.submit()
        time.sleep(0.2)

        info = test_env.client.get_server_info()
        workers = info["workers"]
        assert len(workers) == 1
        assert workers[0]["tasks"] == [(1, 12)]
        assert sorted(workers[0]["objects"]) == [(1, 10), (1, 11)]

        t1.wait()

        info = test_env.client.get_server_info()
        workers = info["workers"]
        assert len(workers) == 1
        assert workers[0]["tasks"] == []
        assert workers[0]["objects"] == [(1, 11)]

        t1.output.unkeep()
        time.sleep(1.15)

        info = test_env.client.get_server_info()
        workers = info["workers"]
        assert len(workers) == 1
        assert workers[0]["tasks"] == []
        assert workers[0]["objects"] == []
