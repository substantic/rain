from rain.client import tasks, blob, TaskException, directory
from rain.client import InputDir, OutputDir
import pytest
import os


def test_sleep1(test_env):
    """Sleep followed by wait"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        b = blob("abc123456")
        t1 = tasks.Sleep(b, 0.3)
        t1.output.keep()
        s.submit()
        test_env.assert_duration(0.2, 0.4, lambda: t1.wait())
        result = test_env.assert_max_duration(0.2,
                                              lambda: t1.output.fetch())
        assert result.get_bytes() == b"abc123456"


def test_sleep2(test_env):
    """Sleep followed by fetch (without explicit wait)"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.Sleep(blob("abc123456"), 0.3)
        t1.output.keep()
        s.submit()
        result = test_env.assert_duration(0.028, 0.45,
                                          lambda: t1.output.fetch())
        assert result.get_bytes() == b"abc123456"


def test_concat1(test_env):
    """Merge several short blobs"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.Concat([blob(x)
                           for x in ("Hello ", "", "", "world", "!", "")])
        t1.output.keep()
        s.submit()
        assert t1.output.fetch().get_bytes() == b"Hello world!"


def test_concat2(test_env):
    """Merge empty list of blobs"""
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.Concat(())
        t1.output.keep()
        s.submit()
        assert t1.output.fetch().get_bytes() == b""


def test_concat3(test_env):
    """Merge large blobs"""
    test_env.start(1)
    a = b"a123456789" * 1024 * 1024
    b = b"b43" * 2500000
    c = b"c" * 1000
    d = b"d"
    with test_env.client.new_session() as s:
        t1 = tasks.Concat((blob(a), blob(c), blob(d), blob(b), blob(c), blob(a)))
        t1.output.keep()
        s.submit()
        assert t1.output.fetch().get_bytes() == a + c + d + b + c + a


def test_chain_concat(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.Concat((blob("a"), blob("b")))
        t2 = tasks.Concat((t1, blob("c")))
        t3 = tasks.Concat((t2, blob("d")))
        t4 = tasks.Concat((t3, blob("e")))
        t5 = tasks.Concat((t4, blob("f")))
        t5.output.keep()
        s.submit()
        assert t5.output.fetch().get_bytes() == b"abcdef"


def test_sleep3_last(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.Sleep(blob("b"), 0.2)
        t2 = tasks.Sleep(t1, 0.2)
        t3 = tasks.Sleep(t2, 0.2)
        s.submit()
        test_env.assert_duration(0.4, 0.8, lambda: t3.wait())


def test_task_open_not_absolute(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.Load("not/absolute/path")
        s.submit()
        pytest.raises(TaskException, lambda: t1.wait())


def test_task_open_not_exists(test_env):
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.Load("/not/exists")
        s.submit()
        pytest.raises(TaskException, lambda: t1.wait())


def test_task_open_ok(test_env):
    import os.path
    path = os.path.abspath(__file__)
    with open(path, "rb") as f:
        content = f.read()
    test_env.start(1)
    with test_env.client.new_session() as s:
        t1 = tasks.Load(path)
        t1.output.keep()
        s.submit()
        assert t1.output.fetch().get_bytes() == content


def test_task_export(test_env):
    import os.path
    test1 = os.path.join(test_env.work_dir, "TEST1")
    test2 = os.path.join(test_env.work_dir, "TEST2")
    test_env.start(1)
    with test_env.client.new_session() as s:
        a = blob("Hello ")
        b = blob("World!")
        tasks.Store(tasks.Concat((a, b)), test1)
        tasks.Store(tasks.Execute("ls /", stdout="output"), test2)
        s.submit()
        s.wait_all()
        with open(test1) as f:
            assert f.read() == "Hello World!"
        with open(test2) as f:
            assert "bin" in f.read()


def test_slice_directory1(test_env):
    os.mkdir("toplevel")
    with open("toplevel/file1.txt", "w") as f:
        f.write("My data 1")
    os.mkdir("toplevel/dir1")
    os.mkdir("toplevel/dir1/dir2")
    with open("toplevel/dir1/dir2/file2.txt", "w") as f:
        f.write("My data 2")

    test_env.start(1, delete_list_timeout=0)
    with test_env.client.new_session() as s:
        d = directory("toplevel")
        a1 = tasks.SliceDirectory(d, "file1.txt")
        a1.output.keep()
        a2 = tasks.SliceDirectory(d, "dir1/")
        a2.output.keep()
        a3 = tasks.SliceDirectory(d, "dir1/")
        a3.output.keep()
        a4 = tasks.SliceDirectory(d, "dir1/dir2/file2.txt")
        a4.output.keep()
        s.submit()
        assert b"My data 1" == a1.output.fetch().get_bytes()
        a2.output.fetch().write("result2")
        with open("result2/dir2/file2.txt") as f:
            assert f.read() == "My data 2"
        a3.output.fetch().write("result3")
        with open("result2/dir2/file2.txt") as f:
            assert f.read() == "My data 2"
        assert b"My data 2" == a4.output.fetch().get_bytes()


def test_slice_directory2(test_env):
    os.mkdir("toplevel")
    with open("toplevel/file1.txt", "w") as f:
        f.write("My data 1")
    os.mkdir("toplevel/dir1")
    os.mkdir("toplevel/dir1/dir2")
    with open("toplevel/dir1/dir2/file2.txt", "w") as f:
        f.write("My data 2")

    test_env.start(1, delete_list_timeout=0)
    with test_env.client.new_session() as s:
        d = directory("toplevel")
        # Force fs mapping
        d = tasks.Execute("ls",
                          input_paths=[InputDir("d", dataobj=d)],
                          output_paths=[OutputDir("d")])
        a1 = tasks.SliceDirectory(d, "file1.txt")
        a1.output.keep()
        a2 = tasks.SliceDirectory(d, "dir1/")
        a2.output.keep()
        a3 = tasks.SliceDirectory(d, "dir1/")
        a3.output.keep()
        a4 = tasks.SliceDirectory(d, "dir1/dir2/file2.txt")
        a4.output.keep()
        s.submit()
        assert b"My data 1" == a1.output.fetch().get_bytes()
        a2.output.fetch().write("result2")
        with open("result2/dir2/file2.txt") as f:
            assert f.read() == "My data 2"
        a3.output.fetch().write("result3")
        with open("result2/dir2/file2.txt") as f:
            assert f.read() == "My data 2"
        assert b"My data 2" == a4.output.fetch().get_bytes()


def test_make_directory(test_env):
    test_env.start(1, delete_list_timeout=0)

    #  TODO: EMPTY DIR os.mkdir("empty")
    os.mkdir("mydir3")
    with open("mydir3/file.txt", "w") as f:
        f.write("My data 4")

    with test_env.client.new_session() as s:
        b1 = blob(b"My data 1")
        b2 = blob(b"My data 2")
        b3 = blob(b"My data 3")
        d1 = directory("mydir3")
        #  TODO: EMPTY DIR d2 = directory("empty")

        t0 = tasks.Execute(
            ["/bin/cat", b1],
            stdout=True,
            input_paths=[InputDir("d1", dataobj=d1)],
            output_paths=[OutputDir("d1")])
        r = tasks.MakeDirectory([
            ("myfile1", t0.outputs["stdout"]),
            ("mydir/mydir2/myfile2", b2),
            ("mydir/myfile3", b3),
            ("mydir/d1a", d1),
            #  ("mydir/d2", d2),
            ("mydir/d1b", t0.outputs["d1"]),
        ])
        r.output.keep()
        s.submit()
        s.wait_all()
        r.output.fetch().write("rdir")
        with open(os.path.join(test_env.work_dir, "rdir", "myfile1")) as f:
            assert f.read() == "My data 1"
        with open(os.path.join(test_env.work_dir, "rdir", "mydir", "mydir2", "myfile2")) as f:
            assert f.read() == "My data 2"
        with open(os.path.join(test_env.work_dir, "rdir", "mydir", "myfile3")) as f:
            assert f.read() == "My data 3"
        with open(os.path.join(test_env.work_dir, "rdir", "mydir", "myfile3")) as f:
            assert f.read() == "My data 3"
        with open(os.path.join(test_env.work_dir, "rdir", "mydir", "d1a", "file.txt")) as f:
            assert f.read() == "My data 4"
        with open(os.path.join(test_env.work_dir, "rdir", "mydir", "d1b", "file.txt")) as f:
            assert f.read() == "My data 4"
        #  TODO: assert os.path.isdir(os.path.join(test_env.work_dir, "rdir", "mydir", "d2"))
