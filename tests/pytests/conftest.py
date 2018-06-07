
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
    default_http_port = "17011"
    running_port = None

    def __init__(self):
        Env.__init__(self)
        self._client = None
        self.governor_defs = None
        self.id_counter = 1

        self.server = None
        self.governors = []
        self.do_final_check = True

    @property
    def work_dir(self):
        return WORK_DIR

    def no_final_check(self):
        self.do_final_check = False

    def start(self,
              n_governors=None,
              n_cpus=1,
              listen_addr=None,
              listen_port=None,
              http_port=None,
              governor_defs=None,
              delete_list_timeout=None,
              executor=None):
        """
        Start infrastructure: server & n governors
        """

        config = None
        if executor:
            name, path = executor
            path = os.path.join(ROOT, path)
            if not os.path.isfile(path):
                raise Exception("Cannot find executor binary: {}".format(path))

            config = "[executors.{}]\n" \
                     "      command = \"{}\"\n".format(name, path)

        if config:
            with open(os.path.join(WORK_DIR, "governor.config"), "w") as f:
                f.write(config)

        env = os.environ.copy()
        env["RUST_LOG"] = "trace"
        env["RUST_BACKTRACE"] = "1"
        env["RAIN_TEST_MODE"] = "1"
        env["RAIN_DEBUG_MODE"] = "1"
        env["PYTHONPATH"] = PYTHON_DIR

        if delete_list_timeout is not None:
            env["RAIN_DELETE_LIST_TIMEOUT"] = str(delete_list_timeout)

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

        if not http_port:
            http_port = self.default_http_port

        server_ready_file = os.path.join(WORK_DIR, "server-ready")

        assert (n_governors is None) != (governor_defs is None)
        if n_governors is not None:
            governor_defs = (n_cpus,) * n_governors

        assert self.governor_defs is None
        self.governor_defs = governor_defs

        # Start SERVER
        args = (RAIN_BIN, "server",
                "--ready-file", server_ready_file,
                "--logdir", os.path.join(WORK_DIR, "server"),
                "--listen", str(addr),
                "--http-listen", str(http_port))
        self.server = self.start_process("server", args, env=env)
        assert self.server is not None

        it = 0
        while not os.path.isfile(server_ready_file):
            time.sleep(0.05)
            self.check_running_processes()
            it += 1
            if it > 100:
                raise Exception("Server not started after 5 s (watching {!r})"
                                .format(server_ready_file))

        # Start GOVERNORS
        self.governors = []

        governor_ready_files = []
        for i, cpus in enumerate(governor_defs):
            name = "governor{}".format(i)
            ready_file = os.path.join(WORK_DIR, name + "-ready")
            governor_ready_files.append(ready_file)
            wdir = os.path.join(WORK_DIR, "governor-{}".format(i))
            args = [RAIN_BIN,
                    "governor", "127.0.0.1:" + str(port),
                    "--ready-file", ready_file,
                    "--cpus", str(cpus),
                    "--logdir", os.path.join(wdir, "logs"),
                    "--workdir", os.path.join(wdir, "work")]
            if config:
                args += ["--config", "governor.config"]
            self.governors.append(self.start_process(name, args, env=env))

        it = 0
        while not all(os.path.isfile(f) for f in governor_ready_files):
            time.sleep(0.05)
            self.check_running_processes()
            it += 1
            if it > 100:
                raise Exception("Governors not started after 5 s")

        self.check_running_processes()

    def check_running_processes(self):
        """Checks that everything is still running"""
        for i, governor in enumerate(self.governors):
            if governor.poll() is not None:
                self.governors = []
                raise Exception(
                    "Governor {0} crashed "
                    "(log in {1}/governor{0}.out; "
                    "Note: If you are running more tests, "
                    "log may be overridden or deleted)".format(i, WORK_DIR))

        if self.server and self.server.poll() is not None:
            self.server = None
            raise Exception(
                "Server crashed (log in {}/server.out; "
                "Note: If you are running more tests, "
                "log may be overridden or deleted)".format(WORK_DIR))

    @property
    def client(self):
        if self._client is not None:
            return self._client
        import rain.client  # noqa
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
            governors = info["governors"]
            assert len(governors) == len(self.governor_defs)
            for w in governors:
                assert not w["tasks"]
                invalid = [o for o in w["objects"] if o not in w["objects_to_delete"]]
                assert not invalid

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
        for root, dirs, files in os.walk(WORK_DIR):
            for d in dirs:
                os.chmod(os.path.join(root, d), 0o700)
            for f in files:
                os.chmod(os.path.join(root, f), 0o700)
        for item in os.listdir(WORK_DIR):
            path = os.path.join(WORK_DIR, item)
            if os.path.isfile(path):
                os.unlink(path)
            else:
                shutil.rmtree(path)
    else:
        os.makedirs(WORK_DIR)
    os.chdir(WORK_DIR)


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
    import rain.client  # noqa
    global id_counter
    id_counter += 1
    return rain.client.session.Session(None, id_counter)
