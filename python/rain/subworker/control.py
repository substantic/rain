import cloudpickle

from .rpc import subworker as rpc_subworker
from rain.client.rpc import common as rpc_common


class ControlImpl(rpc_subworker.SubworkerControl.Server):

    def __init__(self, subworker):
        self.subworker = subworker

    def runTask(self, task, _context):
        params = _context.params

        # Just hack for now
        result = cloudpickle.loads(params.task.taskConfig)()

        objects = _context.results.init("objects", 1)
        objects[0].type = rpc_common.DataObjectType.blob
        objects[0].storage.memory = result
