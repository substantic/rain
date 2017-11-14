from rain.client import Program, Input, Output
from rain.client import RainException

import pytest


def test_program_construction():
    program = Program("/s/bin right --now /here 'and there'")
    assert program.args == ("/s/bin", "right", "--now", "/here", "and there")

    program = Program(["test", "-x",
                       Input("model", "model.txt"),
                       Output("result", "result.dat")])
    assert program.args == ("test", "-x", "model.txt", "result.dat")
    assert program.input_paths == ["model.txt"]
    assert program.input_labels == ["model"]
    assert program.output_paths == ["result.dat"]
    assert program.output_labels == ["result"]


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
        t1.out.output.keep()
        s.submit()
        assert b"etc\n" in t1.out.output.fetch()


def test_program_create_file(test_env):
    """Capturing file"""
    test_env.start(1)
    args = ("/bin/bash", "-c", "echo ABC > output.txt")
    program = Program(args)
    program.output("my_output", "output.txt")
    with test_env.client.new_session() as s:
        t1 = program()
        t1.out.my_output.keep()
        s.submit()
        assert t1.out.my_output.fetch() == b"ABC\n"


def test_program_input_file(test_env):
    """Setting input file for program"""
    test_env.start(1)
    program = Program(("/bin/grep", "ab", Input("in1")), stdout="output")
    with test_env.client.new_session() as s:
        t1 = program(in1="abc\nNOTHING\nabab")
        t1.out.output.keep()
        s.submit()
        assert t1.out.output.fetch() == b"abc\nabab\n"


def test_program_stdin(test_env):
    """Setting input file for program"""
    test_env.start(1)
    args = ("/bin/grep", "ab")
    program = Program(args, stdin="inp", stdout="output")
    with test_env.client.new_session() as s:
        t1 = program(inp="abc\nNOTHING\nabab")
        t1.out.output.keep()
        s.submit()
        assert t1.out.output.fetch() == b"abc\nabab\n"


def test_program_vars(test_env):
    program = Program(("/bin/grep", "${pattern}", "input.txt"),
                      vars=("pattern",), stdout="output")
    program.input("input", "input.txt")
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = program(input="abc\nNOTHING\nabab", pattern="abab")
        t1.out.output.keep()
        s.submit()
        assert t1.out.output.fetch() == b"abab\n"


def test_program_invalid_filename(test_env):
    """Setting input file for program"""
    test_env.start(1)
    args = ("/bin/non-existing-program",)
    program = Program(args, stdout="output")
    with test_env.client.new_session() as s:
        t1 = program()
        t1.out.output.keep()
        s.submit()
        pytest.raises(RainException, lambda: t1.wait())
