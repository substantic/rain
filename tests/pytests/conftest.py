
import os
import os.path
import sys
import subprocess
import time
import shutil
import pytest
import signal

PYTEST_DIR = os.path.dirname(__file__)
ROOT = os.path.dirname(os.path.dirname(PYTEST_DIR))
PYTHON_DIR = os.path.join(ROOT, "python")
WORK_DIR = os.path.join(PYTEST_DIR, "work")
RAIN_BIN = os.environ.get("RAIN_TEST_BIN",
                          os.path.join(ROOT, "target", "debug", "rain"))

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
                                     preexec_fn=os.setsid,
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
            # Kill the whole group since the process may spawn a child
            if not p.poll():
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)


class TestEnv(Env):

    default_listen_port = "17010"
    running_port = None

    def __init__(self):
        Env.__init__(self)
        self._client = None
        self.n_workers = None
        self.id_counter = 1

        self.server = None
        self.workers = []
        self.do_final_check = True

    def no_final_check(self):
        self.do_final_check = False

    def start(self, n_workers=1, n_cpus=1, listen_addr=None, listen_port=None):
        """
        Start infrastructure: server & n workers
        """
        assert self.n_workers is None
        self.n_workers = n_workers
        env = os.environ.copy()
        env["RUST_LOG"] = "trace"
        env["RUST_BACKTRACE"] = "1"
        env["PYTHONPATH"] = PYTHON_DIR

        if listen_addr:
            if listen_port:
                addr = listen_addr + ":" + listen_port
                port = listen_port
            else:
                addr = listen_addr
                port = self.default_listen_port
        else:
            if listen_port:
                addr = listen_port
                port = listen_port
            else:
                addr = self.default_listen_port
                port = self.default_listen_port
        self.running_port = port

        # Start SERVER
        args = (RAIN_BIN, "server", "--listen", str(addr))
        server = self.start_process("server", args, env=env)
        time.sleep(0.1)
        if server.poll():
            raise Exception("Server is not running")

        # Start WORKERS
        workers = []
        args = (RAIN_BIN,
                "worker", "127.0.0.1:" + str(port),
                "--cpus", str(n_cpus),
                "--workdir", WORK_DIR)
        for i in range(n_workers):
            name = "worker{}".format(i)
            workers.append(self.start_process(name, args, env=env))
        time.sleep(0.2)

        self.server = server
        self.workers = workers

        self.check_running_processes()

    def check_running_processes(self):
        """Checks that everything is still running"""
        for i, worker in enumerate(self.workers):
            if worker.poll():
                self.workers = []
                raise Exception(
                    "Worker {0} crashed "
                    "(log in {1}/worker{0}.out; "
                    "Note: If you are running more tests, "
                    "log may be overridden or deleted)".format(i, WORK_DIR))
        if self.server and self.server.poll():
            self.server = None
            raise Exception(
                "Server crashed (log in {}/server.out; "
                "Note: If you are running more tests, "
                "log may be overridden or deleted)".format(WORK_DIR))

    @property
    def client(self):
        if self._client is not None:
            return self._client
        import rain  # noqa
        if self.running_port is None:
            raise Exception("Server was not started in test environment")
        client = rain.client.Client("127.0.0.1", self.running_port)
        self._client = client
        return client

    def final_check(self):
        if not self.do_final_check:
            return
        if self._client:
            time.sleep(0.1)
            info = self._client.get_server_info()
            workers = info["workers"]
            assert len(workers) == self.n_workers
            for w in workers:
                assert w["n_tasks"] == 0
                assert w["n_objects"] == 0

    def close(self):
        self._client = None

    def assert_duration(self, min_seconds, max_seconds, fn):
        start = time.time()
        result = fn()
        diff = time.time() - start
        assert min_seconds <= diff <= max_seconds
        return result

    def assert_min_duration(self, seconds, fn):
        start = time.time()
        result = fn()
        diff = time.time() - start
        assert diff >= seconds
        return result

    def assert_max_duration(self, seconds, fn):
        start = time.time()
        result = fn()
        diff = time.time() - start
        assert diff <= seconds
        return result


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
    time.sleep(0.2)
    try:
        env.final_check()
        env.check_running_processes()
    finally:
        env.close()
        env.kill_all()
        # Final sleep to let server port be freed, on some slow computers
        # a new test is starter before the old server is properly cleaned
        time.sleep(0.02)


id_counter = 0


@pytest.fixture
def fake_session():
    """Returns a new fake session for tests that do not need any server"""
    import rain  # noqa
    global id_counter
    id_counter += 1
    return rain.client.session.Session(None, id_counter)
