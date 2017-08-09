from .task import Task
import struct


def concat(*objs):
    """Creates a task that Concatenate data objects"""
    return Task("concat", inputs=objs)


def sleep(timeout, dataobj):
    """Task that forwards argument 'dataobj' after 'timeout' seconds.
    This task serves for testing purpose"""
    time_ms = int(timeout * 1000)
    return Task("sleep",
                struct.pack(">I", time_ms),
                inputs=(dataobj,))

