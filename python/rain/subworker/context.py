import shutil
import os.path

from ..common.data_instance import DataInstance
from ..common import RainException
from ..common.content_type import (check_content_type, encode_value)


class Context:

    def __init__(self, subworker):
        self._subworker = subworker
        self._id_counter = 0
        self._staged_paths = set()
        self._debug_messages = []
        self.attributes = {}
        self.function = None

    def stage_file(self, path, content_type=None):
        """Creates DataInstance from file.

           The original file is moved from working directory.
           Path must be relative path with respect to working directory
           of task.
        """
        self._id_counter += 1
        if os.path.isabs(path):
            raise Exception("Path '{}' has to be relative")
        target = os.path.join(
            self._subworker.stage_path, str(self._id_counter))
        shutil.move(path, target)
        data = DataInstance(path=target, content_type=content_type)
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

        return DataInstance(data=value, content_type=content_type)

    def pickled(self, obj, content_type="pickle"):
        return self.blob(obj, encode="pickle")

    def debug(self, message):
        if not isinstance(message, str):
            raise Exception("Method 'debug' accepts only strings")
        self._debug_messages.append(message)

    def _cleanup(self, results):
        for result in results:
            if result in self._staged_paths:
                self._staged_paths.remove(result)

        for data in self._staged_paths:
            data._remove()

    def _cleanup_on_fail(self):
        for data in self._staged_paths:
            data._remove()
