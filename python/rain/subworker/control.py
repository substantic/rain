from .rpc import subworker as rpc_subworker
from rain.client.rpc import common as rpc_common
from .data import data_from_capnp, Data
from .context import Context
from ..common.attributes import attributes_to_capnp
import traceback


def load_worker_object(reader):
    data = data_from_capnp(reader.data)
    data.worker_object_id = (reader.id.sessionId, reader.id.id)
    return data


def write_attributes(context, builder):
    attributes_to_capnp(context.attributes, builder)


class ControlImpl(rpc_subworker.SubworkerControl.Server):

    def __init__(self, subworker):
        self.subworker = subworker

    def runTask(self, task, _context):
        task_context = Context(self.subworker)
        try:
            params = _context.params

            inputs = [load_worker_object(reader)
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
                    raise Exception("Invalid result object: {!r}".format(data))
            task_context._cleanup(task_results)
            write_attributes(task_context, _context.results.taskAttributes)
            _context.results.ok = True

        except Exception:
            task_context._cleanup_on_fail()
            _context.results.errorMessage = traceback.format_exc()
            write_attributes(task_context, _context.results.taskAttributes)
            _context.results.ok = False