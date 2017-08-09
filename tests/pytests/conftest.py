
import os
import os.path
import sys
import subprocess
import time
import shutil
import pytest


PYTEST_DIR = os.path.dirname(__file__)
ROOT = os.path.dirname(os.path.dirname(PYTEST_DIR))
PYTHON_DIR = os.path.join(ROOT, "python")
WORK_DIR = os.path.join(PYTEST_DIR, "work")
RAIN_DEBUG_BIN = os.path.join(ROOT, "target", "debug", "rain")

sys.path.insert(0, PYTHON_DIR)


class Env:

    def __init__(self):
        self.processes = []
        self.cleanups = []

    def start_process(self, name, args, env=None, catch_io=True):
        fname = os.path.join(WORK_DIR, name)
        if catch_io:
            with open(fname + ".out", "w") as out:
                p = subprocess.Popen(args,
                                     stdout=out,
                                     stderr=subprocess.STDOUT,
                                     cwd=WORK_DIR,
                                     env=env)
        else:
            p = subprocess.Popen(args,
                                 cwd=WORK_DIR,
                                 env=env)
        self.processes.append((name, p))
        return p

    def kill_all(self):
        for fn in self.cleanups:
            fn()
        for n, p in self.processes:
            p.kill()


class TestEnv(Env):

    PORT = 17010

    def __init__(self):
        Env.__init__(self)
        self._client = None
        self.n_workers = None
        self.id_counter = 1

    def start(self, n_workers=1):
        """
        Start infrastructure: server & n workers
        """
        env = os.environ.copy()
        env["RUST_BACKTRACE"] = "1"

        # Start SERVER
        args = (RAIN_DEBUG_BIN, "server", "--port", str(self.PORT))
        server = self.start_process("server", args, env=env)
        time.sleep(0.1)
        assert not server.poll()

        # Start WORKERS
        workers = []
        args = (RAIN_DEBUG_BIN,
                "worker", "127.0.0.1:" + str(self.PORT))
        for i in range(n_workers):
            name = "worker{}".format(i)
            workers.append(self.start_process(name, args, env=env))
        time.sleep(0.2)

        # Check that everything is still running
        for worker in workers:
            assert not worker.poll()
        assert not server.poll()

    @property
    def client(self):
        if self._client is not None:
            return self._client
        import rain
        client = rain.Client("127.0.0.1", self.PORT)
        self._client = client
        return client

    def fake_session(self):
        """Returns a new fake session for tests that do not need any server"""
        import rain
        self.id_counter += 1
        return rain.client.session.Session(None, self.id_counter)


    def close(self):
        self._client = None

    def assert_min_duration(self, seconds, fn):
        start = time.time()
        fn()
        diff = time.time() - start
        assert diff >= seconds

    def assert_max_duration(self, seconds, fn):
        start = time.time()
        fn()
        diff = time.time() - start
        assert diff <= seconds


def prepare():
    """Prepare working directory

    If directory exists then it is cleaned;
    If it does not exists then it is created.
    """
    if os.path.isdir(WORK_DIR):
        for item in os.listdir(WORK_DIR):
            path = os.path.join(WORK_DIR, item)
            if os.path.isfile(path):
                os.unlink(path)
            else:
                shutil.rmtree(path)
    else:
        os.makedirs(WORK_DIR)


@pytest.yield_fixture(autouse=True, scope="function")
def test_env():
    """Fixture that allows to start Rain test environment"""
    prepare()
    env = TestEnv()
    yield env
    time.sleep(0.1)
    env.close()
    env.kill_all()
