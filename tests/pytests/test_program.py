from rain.client import rpc, session, tasks
from rain import RainException

import pytest


def test_program_sleep_1(test_env):
    """Sleep followed by wait"""
    test_env.start(1)
    program = tasks.Program("sleep 1")
    with test_env.client.new_session() as s:
        t1 = program()
        s.submit()
        test_env.assert_duration(0.99, 1.1, lambda: t1.wait())


def test_program_stdout(test_env):
    """Capturing stdout"""
    test_env.start(1)
    program = tasks.Program("ls /", stdout="output")
    with test_env.client.new_session() as s:
        t1 = program()
        s.submit()
        assert b"etc\n" in t1.out.output.fetch()


def test_program_create_file(test_env):
    """Capturing file"""
    test_env.start(1)
    args = ("/bin/bash", "-c", "echo ABC > output.txt")
    program = tasks.Program(args, outputs=(("output.txt", "my_output"),))
    with test_env.client.new_session() as s:
        t1 = program()
        s.submit()
        assert t1.out.my_output.fetch() == b"ABC\n"


def test_program_input_file(test_env):
    """Capturing file"""
    test_env.start(1)
    args = ("/bin/grep", "ab", "input.txt")
    program = tasks.Program(args, inputs=(("input.txt", "in1"),), stdout="output")
    with test_env.client.new_session() as s:
        t1 = program(in1="abc\nNOTHING\nabab")
        s.submit()
        assert t1.out.output.fetch() == b"abc\nabab\n"
