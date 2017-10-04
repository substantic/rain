from rain.client import Program

import pytest


def test_program_sleep_1(test_env):
    """Sleep followed by wait"""
    test_env.start(1)
    program = Program("sleep 1")
    with test_env.client.new_session() as s:
        t1 = program()
        s.submit()
        test_env.assert_duration(0.99, 1.1, lambda: t1.wait())


def test_program_stdout_only(test_env):
    """Capturing stdout"""
    test_env.start(1)
    program = Program("ls /", stdout="output")
    with test_env.client.new_session() as s:
        t1 = program()
        s.submit()
        assert b"etc\n" in t1.out.output.fetch()


def test_program_create_file(test_env):
    """Capturing file"""
    test_env.start(1)
    args = ("/bin/bash", "-c", "echo ABC > output.txt")
    program = Program(args)
    program.output("output.txt", "my_output")
    with test_env.client.new_session() as s:
        t1 = program()
        s.submit()
        assert t1.out.my_output.fetch() == b"ABC\n"


def test_program_input_file(test_env):
    """Setting input file for program"""
    test_env.start(1)
    args = ("/bin/grep", "ab", "input.txt")
    program = Program(args, stdout="output")
    program.input("input.txt", "in1")
    with test_env.client.new_session() as s:
        t1 = program(in1="abc\nNOTHING\nabab")
        s.submit()
        assert t1.out.output.fetch() == b"abc\nabab\n"


def test_program_stdin(test_env):
    """Setting input file for program"""
    test_env.start(1)
    args = ("/bin/grep", "ab")
    program = Program(args, stdin="inp", stdout="output")
    with test_env.client.new_session() as s:
        t1 = program(inp="abc\nNOTHING\nabab")
        s.submit()
        assert t1.out.output.fetch() == b"abc\nabab\n"
