
from schedtester import Scenario, Worker
import random

# Size of object considered as big
BIG = 50 * 1024 * 1024  # 50MB

# Size of object considered small
SMALL = 10 * 1024  # 10kB


def test_scheduler_one_to_one(test_env):
    NUMBER_OF_WORKERS = 20
    r = random.Random(b"Rain")
    workers = [Worker(cpus=1) for i in range(NUMBER_OF_WORKERS)]
    s = Scenario(test_env, workers)
    r.shuffle(workers)
    objects = [s.new_object(w, BIG) for w in workers]
    for o, w in zip(objects, workers):
        s.new_task([o], expect_worker=w)
    s.run()


def test_scheduler_big_and_small(test_env):
    NUMBER_OF_WORKERS = 20
    r = random.Random(b"Rain")
    workers = [Worker(cpus=1) for i in range(NUMBER_OF_WORKERS)]
    s = Scenario(test_env, workers)
    r.shuffle(workers)
    objects1 = [s.new_object(w, SMALL) for w in workers]
    r.shuffle(workers)
    objects2 = [s.new_object(w, BIG) for w in workers]
    for o1, o2, w in zip(objects1, objects2, workers):
        inputs = [o1, o2, o1]
        r.shuffle(inputs)
        s.new_task(inputs, expect_worker=w)
    s.run()


def test_scheduler_fit_to_cpus(test_env):
    NUMBER_OF_WORKERS = 6
    r = random.Random(b"Rain")
    workers = [Worker(cpus=i+1) for i in range(NUMBER_OF_WORKERS)]
    r.shuffle(workers)
    s = Scenario(test_env, workers)
    r.shuffle(workers)
    for w in workers:
        s.new_task([], expect_worker=w, cpus=w.cpus)
    s.run()


# o0(all), o1(all), o2(w0)
# | \        /         /
# |  \      / /-------/
# t2   t0, t1              t3 ... t10
#
def test_scheduler_biggest_irrelevant(test_env):

    NUMBER_OF_WORKERS = 3
    r = random.Random(b"Rain")
    workers = [Worker(cpus=2) for i in range(NUMBER_OF_WORKERS)]
    s = Scenario(test_env, workers)
    r.shuffle(workers)
    w0, w1, w2 = workers
    o1 = s.new_object(workers=workers, size=10*BIG)
    o2 = s.new_object(workers=workers, size=10*BIG)
    o3 = s.new_object(workers=w0, size=BIG)

    s.new_task([o1, o2, o3], expect_worker=w0)  # t0
    s.new_task([o1, o2, o3], expect_worker=w0)  # t1
    s.new_task([o1], expect_worker=[w1, w2])    # t2

    # t3 - t10
    for w in range(3, 11):
        s.new_task([])
    s.run()

    import time
    time.sleep(3)


# o0(w0)
#   \
#    \
#      t0      o1(w1)
#       \      /
#        \    /
#          t1
def test_scheduler_big_vs_dynamic_small(test_env):

    NUMBER_OF_WORKERS = 3
    r = random.Random(b"Rain")
    workers = [Worker(cpus=2) for i in range(NUMBER_OF_WORKERS)]
    s = Scenario(test_env, workers)
    r.shuffle(workers)
    w0, w1, w2 = workers
    o1 = s.new_object(workers=w0, size=10*BIG)
    o2 = s.new_object(workers=w1, size=10*BIG)

    t0 = s.new_task([o1], expect_worker=w0)
    s.new_task([t0.output, o2], expect_worker=w1)
    s.run()
