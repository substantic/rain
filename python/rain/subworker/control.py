from .rpc import subworker as rpc_subworker
from rain.client.rpc import common as rpc_common
from .data import data_from_capnp
import traceback


class ControlImpl(rpc_subworker.SubworkerControl.Server):

    def __init__(self, subworker):
        self.subworker = subworker

    def runTask(self, task, _context):
        try:
            params = _context.params
            inputs = [data_from_capnp(reader.data)
                      for reader in params.task.inputs]
            outputs = [reader.label
                       for reader in params.task.outputs]

            result = self.subworker.run_task(
                params.task.taskConfig, inputs, outputs)

            results = _context.results.init("data", len(result))
            for i, data in enumerate(result):
                results[i].type = rpc_common.DataObjectType.blob
                results[i].storage.memory = data
            _context.results.ok = True

        except Exception:
            _context.results.ok = False
            _context.results.errorMessage = traceback.format_exc()
