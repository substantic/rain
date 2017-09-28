from .rpc import subworker as rpc_subworker
from rain.client.rpc import common as rpc_common
from .data import data_from_capnp


class ControlImpl(rpc_subworker.SubworkerControl.Server):

    def __init__(self, subworker):
        self.subworker = subworker

    def runTask(self, task, _context):
        params = _context.params
        inputs = [data_from_capnp(reader.data)
                  for reader in params.task.inputs]
        result = self.subworker.run_task(params.task.taskConfig, inputs)

        results = _context.results.init("data", 1)
        results[0].type = rpc_common.DataObjectType.blob
        results[0].storage.memory = result
