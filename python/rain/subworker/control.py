from .rpc import subworker as rpc_subworker
from rain.client.rpc import common as rpc_common
from .data import data_from_capnp, Data
from .context import Context
import traceback


class ControlImpl(rpc_subworker.SubworkerControl.Server):

    def __init__(self, subworker):
        self.subworker = subworker

    def runTask(self, task, _context):
        task_context = Context(self.subworker)
        try:
            params = _context.params
            inputs = [data_from_capnp(reader.data)
                      for reader in params.task.inputs]
            outputs = [reader.label
                       for reader in params.task.outputs]

            task_results = self.subworker.run_task(
                task_context, params.task.taskConfig, inputs, outputs)

            results = _context.results.init("data", len(task_results))
            for i, data in enumerate(task_results):
                if isinstance(data, Data):
                    data.to_capnp(results[i])
                elif isinstance(data, bytes):
                    results[i].type = rpc_common.DataObjectType.blob
                    results[i].storage.memory = data
                elif isinstance(data, str):
                    results[i].type = rpc_common.DataObjectType.blob
                    results[i].storage.memory = data.encode()
                else:
                    raise Exception("Invalid result object: {}", repr(data))
            task_context.cleanup(task_results)
            _context.results.ok = True

        except Exception:
            task_context.cleanup_on_fail()
            _context.results.ok = False
            _context.results.errorMessage = traceback.format_exc()
