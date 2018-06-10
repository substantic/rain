import shutil
import os.path

from ..common.data_instance import DataInstance
from ..common import RainException, DataType
from ..common.content_type import (check_content_type, encode_value)
from ..common.attributes import TaskInfo


class Context:

    def __init__(self, executor):
        self._executor = executor
        self._id_counter = 0
        self._staged_paths = set()
        self.function = None
        self.spec = None
        self.info = TaskInfo()
        self._debug_messages = []

    def stage_file(self, path, content_type=None):
        """Creates DataInstance from file.

           The file is moved out of the working directory.
           Path must be relative path with respect to working directory
           of task.
        """
        self._id_counter += 1
        if os.path.isabs(path):
            raise Exception("Path '{}' has to be relative")
        target = os.path.join(
            self._executor.stage_path, str(self._id_counter))
        shutil.move(path, target)
        data = DataInstance(path=target, content_type=content_type, data_type=DataType.BLOB)
        self._staged_paths.add(data)
        return data

    def stage_directory(self, path):
        """Creates DataInstance from directory

           The directory is moved out of the working directory.
           Path must be relative path with respect to working directory
           of task.
        """
        self._id_counter += 1
        if os.path.isabs(path):
            raise Exception("Path '{}' has to be relative")
        target = os.path.join(
            self._executor.stage_path, str(self._id_counter))
        os.rename(path, target)
        data = DataInstance(path=target, content_type="dir", data_type=DataType.DIRECTORY)
        self._staged_paths.add(data)
        return data

    def blob(self, value, content_type=None, encode=None):
        if content_type is not None:
            assert encode is None, "Specify only one of `content_type` and `encode`"
            assert isinstance(value, bytes), "`content_type` only allowed for `bytes`"

        if encode is None and isinstance(value, str):
            encode = "text:utf-8"
            assert content_type is None, "content_type not allowed for `str`"

        if encode is not None:
            check_content_type(encode)
            value = encode_value(value, content_type=encode)
            content_type = encode

        if not isinstance(value, bytes):
            raise RainException(
                "Invalid blob type (only str or bytes allowed without `encode`)")

        return DataInstance(data=value, content_type=content_type, data_type=DataType.BLOB)

    def pickled(self, obj, content_type="pickle"):
        return self.blob(obj, encode="pickle")

    def debug(self, message, *args, **kw):
        """ Add a message to debug stream that is returned as task attribute "debug".
            *args and **kw is used to call .format(*args, **kw) on 'message'.
            Server prints "debug" attribute when task fails, but it can be also
            accessed as normal attribute """
        if not isinstance(message, str):
            raise Exception("First argument has to be a string")
        self._debug_messages.append(message.format(*args, **kw))

    def _cleanup(self, results):
        for result in results:
            if result in self._staged_paths:
                self._staged_paths.remove(result)

        for data in self._staged_paths:
            data._remove()

    def _cleanup_on_fail(self):
        for data in self._staged_paths:
            data._remove()
