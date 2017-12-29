import shutil
import os.path

from .data import DataInstance
from ..common import packing


class Context:

    def __init__(self, subworker):
        self._subworker = subworker
        self._id_counter = 0
        self._staged_paths = set()
        self._debug_messages = []
        self.attributes = {}

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

    def blob(self, data=None, content_type=None):
        return DataInstance(data, content_type=content_type)

    def dump(self, obj, content_type="py"):
        return self.blob(packing.dump_mem(obj, content_type),
                         content_type=content_type)

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
