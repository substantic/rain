import capnp
import os

from .session import Session

CLIENT_PROTOCOL_VERSION = 0

SRC_DIR = os.path.dirname(__file__)
capnp.remove_import_hook()
common_capnp = capnp.load(SRC_DIR + "/../../../capnp/common.capnp")
server_capnp = capnp.load(SRC_DIR + "/../../../capnp/server.capnp")


class Client:

    def __init__(self, address, port):
        self.submit_id = 0
        self.handles = {}
        self.rpc_client = capnp.TwoPartyClient("{}:{}".format(address, port))

        bootstrap = self.rpc_client.bootstrap().cast_as(server_capnp.ServerBootstrap)
        registration = bootstrap.registerAsClient(CLIENT_PROTOCOL_VERSION)
        self.service = registration.wait().service

    def new_session(self):
        session_id = self.service.newSession().wait().sessionId
        return Session(self, session_id)

    def get_server_info(self):
        """ Returns basic server info """
        info = self.service.getServerInfo().wait()
        return {
            "n_workers": info.nWorkers
        }

    def _submit(self, tasks, dataobjs):
        req = self.service.submit_request()
        req.init("tasks", len(tasks))
        for i in range(len(tasks)):
            t = req.tasks[i]
            t_py = tasks[i]
            t.id.id = t_py.id
            t.id.sessionId = t_py.session.session_id
            t.init("inputs", len(t_py.inputs))
            for tii in range(len(t_py.inputs)):
                t.inputs[tii].id.id = t_py.inputs[tii].id
                t.inputs[tii].id.sessionId = t_py.inputs[tii].session.session_id
                t.inputs[tii].label = ""
            t.init("outputs", len(t_py.outputs))
            toi = 0
            for out_label, out_task in t_py.outputs.items():
                t.outputs[toi].id.id = out_task.id
                t.outputs[toi].label = out_label
                toi += 1
            t.taskType = t_py.task_type
            if t_py.task_config:
                t.taskConfig = t_py.task_config
            t.nCpus = t_py.nCpus
            t.taskType = t_py.task_type
            t_py.state = common_capnp.TaskState.notAssigned
        req.init("objects", len(dataobjs))
        for i in range(len(dataobjs)):
            obj = req.objects[i]
            obj_py = dataobjs[i]
            obj.id.id = obj_py.id
            obj.keep = obj_py.is_kept()
            if obj_py.data:
                obj.data = obj_py.data
            obj_py.state = common_capnp.DataObjectState.notAssigned
        req.send().wait()
