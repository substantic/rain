from .task import Task
from .data import to_data

import struct


def concat(*objs):
    """Creates a task that Concatenate data objects"""
    return Task("!concat", inputs=objs)


def sleep(timeout, dataobj):
    """Task that forwards argument 'dataobj' after 'timeout' seconds.
    The type of resulting data object is the same as type of input data object
    This task serves for testing purpose"""
    time_ms = int(timeout * 1000)
    dataobj = to_data(dataobj)
    return Task("!sleep",
                struct.pack("<I", time_ms),
                inputs=(dataobj,),
                outputs=(dataobj.__class__("output"),))