
from schedtester import Scenario, Governor
import random

# Size of object considered as big
BIG = 50 * 1024 * 1024  # 50MB

# Size of object considered small
SMALL = 10 * 1024  # 10kB


def test_scheduler_one_to_one(test_env):
    NUMBER_OF_GOVERNORS = 20
    r = random.Random(b"Rain")
    governors = [Governor(cpus=1) for i in range(NUMBER_OF_GOVERNORS)]
    s = Scenario(test_env, governors)
    r.shuffle(governors)
    objects = [s.new_object(w, BIG) for w in governors]
    for o, w in zip(objects, governors):
        s.new_task([o], expect_governor=w)
    s.run()


def test_scheduler_big_and_small(test_env):
    NUMBER_OF_GOVERNORS = 20
    r = random.Random(b"Rain")
    governors = [Governor(cpus=1) for i in range(NUMBER_OF_GOVERNORS)]
    s = Scenario(test_env, governors)
    r.shuffle(governors)
    objects1 = [s.new_object(w, SMALL) for w in governors]
    r.shuffle(governors)
    objects2 = [s.new_object(w, BIG) for w in governors]
    for o1, o2, w in zip(objects1, objects2, governors):
        inputs = [o1, o2, o1]
        r.shuffle(inputs)
        s.new_task(inputs, expect_governor=w)
    s.run()


def test_scheduler_fit_to_cpus(test_env):
    NUMBER_OF_GOVERNORS = 6
    r = random.Random(b"Rain")
    governors = [Governor(cpus=i+1) for i in range(NUMBER_OF_GOVERNORS)]
    r.shuffle(governors)
    s = Scenario(test_env, governors)
    r.shuffle(governors)
    for w in governors:
        s.new_task([], expect_governor=w, cpus=w.cpus)
    s.run()


# o0(all), o1(all), o2(w0)
# | \        /         /
# |  \      / /-------/
# t2   t0, t1              t3 ... t10
#
def test_scheduler_biggest_irrelevant(test_env):

    NUMBER_OF_GOVERNORS = 3
    r = random.Random(b"Rain")
    governors = [Governor(cpus=2) for i in range(NUMBER_OF_GOVERNORS)]
    s = Scenario(test_env, governors)
    r.shuffle(governors)
    w0, w1, w2 = governors

    print("w0 =", w0.governor_id)
    print("w1 =", w1.governor_id)
    print("w2 =", w2.governor_id)

    o1 = s.new_object(governors=governors, size=10*BIG)
    o2 = s.new_object(governors=governors, size=10*BIG)
    o3 = s.new_object(governors=w0, size=BIG)

    s.new_task([o1, o2, o3], expect_governor=w0, label="t0")  # t0
    s.new_task([o1, o2, o3], expect_governor=w0, label="t1")  # t1
    # s.new_task([o1], expect_governor=[w1, w2], label="t2")    # t2

    # t3 - t10
    for w in range(3, 11):
        s.new_task([], label="t" + str(w))
    s.run()


# o0(w0)
#   \
#    \
#      t0      o1(w1)
#       \      /
#        \    /
#          t1
def test_scheduler_big_vs_dynamic_small(test_env):

    NUMBER_OF_GOVERNORS = 3
    r = random.Random(b"Rain")
    governors = [Governor(cpus=2) for i in range(NUMBER_OF_GOVERNORS)]
    s = Scenario(test_env, governors)
    r.shuffle(governors)
    w0, w1, w2 = governors
    o1 = s.new_object(governors=w0, size=10*BIG)
    o2 = s.new_object(governors=w1, size=10*BIG)

    t0 = s.new_task([o1], expect_governor=w0)
    s.new_task([t0.output, o2], expect_governor=w1)
    s.run()
