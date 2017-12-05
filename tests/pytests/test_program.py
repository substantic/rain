from rain.client import Program, Input, Output, tasks, blob
from rain.client import RainException

import os
import pytest


def test_execute_positional_input(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t0 = tasks.execute("ls /", stdout=True)
        t1 = tasks.execute(("split", "-d", "-n", "l/2", t0),
                           outputs=["x00", "x01"])
        t1.outputs["x00"].keep()
        t1.outputs["x01"].keep()
        s.submit()
        t1.outputs["x00"].fetch()
        t1.outputs["x01"].fetch()


def test_execute_positional_output(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t0 = tasks.execute("ls /", stdout=True)
        t1 = tasks.execute(("tee", Output("file")), stdin=t0, stdout="stdout")
        t1.outputs["file"].keep()
        t1.outputs["stdout"].keep()
        s.submit()
        f = t1.outputs["file"].fetch()
        s = t1.outputs["stdout"].fetch()
        assert f == s


def test_execute_sleep_1(test_env):
    """Sleep followed by wait"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.execute("sleep 1")
        s.submit()
        test_env.assert_duration(0.99, 1.1, lambda: t1.wait())


def test_program_sleep_1(test_env):
    """Sleep followed by wait"""
    test_env.start(1)
    program = Program("sleep 1")
    with test_env.client.new_session() as s:
        t1 = program()
        s.submit()
        test_env.assert_duration(0.99, 1.1, lambda: t1.wait())


def test_execute_stdout_only(test_env):
    """Capturing stdout"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.execute("ls /", stdout="output")
        t1.output.keep()
        s.submit()
        assert b"etc\n" in t1.output.fetch()


def test_program_stdout_only(test_env):
    """Capturing stdout"""
    test_env.start(1)
    program = Program("ls /", stdout="output")
    with test_env.client.new_session() as s:
        t1 = program()
        t1.output.keep()
        s.submit()
        assert b"etc\n" in t1.output.fetch()


def test_execute_create_file(test_env):
    """Capturing file"""
    test_env.start(1)
    args = ("/bin/bash", "-c", "echo ABC > output.txt")
    with test_env.client.new_session() as s:
        t1 = tasks.execute(args, outputs=[Output("my_output", "output.txt")])
        t1.outputs["my_output"].keep()
        s.submit()
        assert t1.outputs["my_output"].fetch() == b"ABC\n"


def test_program_create_file(test_env):
    """Capturing file"""
    test_env.start(1)
    args = ("/bin/bash", "-c", "echo ABC > output.txt")
    program = Program(args, outputs=[Output("my_output", "output.txt")])
    with test_env.client.new_session() as s:
        t1 = program()
        t1.outputs["my_output"].keep()
        s.submit()
        assert t1.outputs["my_output"].fetch() == b"ABC\n"


def test_execute_input_file(test_env):
    """Setting input file for program"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.execute(("/bin/grep", "ab",
                            Input("in1", data=blob("abc\nNOTHING\nabab"))),
                           stdout="output")
        t1.output.keep()
        s.submit()
        assert t1.output.fetch() == b"abc\nabab\n"


def test_program_input_file(test_env):
    """Setting input file for program"""
    test_env.start(1)
    program = Program(("/bin/grep", "ab", Input("in1")), stdout="output")
    with test_env.client.new_session() as s:
        t1 = program(in1=blob("abc\nNOTHING\nabab"))
        t1.output.keep()
        s.submit()
        assert t1.output.fetch() == b"abc\nabab\n"


def test_execute_stdin(test_env):
    """Setting input file for program"""
    test_env.start(1)
    args = ("/bin/grep", "ab")
    with test_env.client.new_session() as s:
        t1 = tasks.execute(args, stdin=blob("abc\nNOTHING\nabab"),
                           stdout="output")
        t1.output.keep()
        s.submit()
        assert t1.output.fetch() == b"abc\nabab\n"


def test_program_stdin(test_env):
    """Setting input file for program"""
    test_env.start(1)
    args = ("/bin/grep", "ab")
    program = Program(args, stdin="inp", stdout="output")
    with test_env.client.new_session() as s:
        t1 = program(inp=blob("abc\nNOTHING\nabab"))
        t1.output.keep()
        s.submit()
        assert t1.output.fetch() == b"abc\nabab\n"


def test_execute_invalid_filename(test_env):
    """Setting input file for program"""
    test_env.start(1)
    args = ("/bin/non-existing-program",)
    with test_env.client.new_session() as s:
        t1 = tasks.execute(args, stdout="output")
        t1.output.keep()
        s.submit()
        pytest.raises(RainException, lambda: t1.wait())


def test_program_invalid_filename(test_env):
    """Setting input file for program"""
    test_env.start(1)
    args = ("/bin/non-existing-program",)
    program = Program(args, stdout="output")
    with test_env.client.new_session() as s:
        t1 = program()
        t1.output.keep()
        s.submit()
        pytest.raises(RainException, lambda: t1.wait())


def test_execute_fail(test_env):
    """Setting input file for program"""
    test_env.start(1)
    args = ("ls", "/non-existing-dir")
    with test_env.client.new_session() as s:
        t1 = tasks.execute(args, stdout="output")
        s.submit()
        pytest.raises(RainException, lambda: t1.wait())


def test_program_fail(test_env):
    """Setting input file for program"""
    test_env.start(1)
    args = ("ls", "/non-existing-dir")
    program = Program(args, stdout="output")
    with test_env.client.new_session() as s:
        t1 = program()
        s.submit()
        pytest.raises(RainException, lambda: t1.wait())


def test_execute_shell(test_env):
    test_env.start(1)
    p1 = Program(("echo", "$HOME"), stdout=True)
    p2 = Program(("echo", "$HOME"), stdout=True, shell=True)

    with test_env.client.new_session() as s:
        t1 = tasks.execute(("echo", "$HOME"), stdout=True)
        t1.output.keep()
        t2 = tasks.execute(("echo", "$HOME"), stdout=True, shell=True)
        t2.output.keep()
        t3 = p1()
        t3.output.keep()
        t4 = p2()
        t4.output.keep()
        s.submit()
        assert b"$HOME\n" == t1.output.fetch()
        assert (os.getenv("HOME") + "\n").encode() == t2.output.fetch()
        assert b"$HOME\n" == t3.output.fetch()
        assert (os.getenv("HOME") + "\n").encode() == t4.output.fetch()
