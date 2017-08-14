import capnp
from rain.client import rpc


from .session import Session

CLIENT_PROTOCOL_VERSION = 0


class Client:

    def __init__(self, address, port):
        self.submit_id = 0
        self.handles = {}
        self.rpc_client = capnp.TwoPartyClient("{}:{}".format(address, port))

        bootstrap = self.rpc_client.bootstrap().cast_as(rpc.server.ServerBootstrap)
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

        # Serialize tasks
        req.init("tasks", len(tasks))
        for i in range(len(tasks)):
            tasks[i].to_capnp(req.tasks[i])

        # Serialize objects
        req.init("objects", len(dataobjs))
        for i in range(len(dataobjs)):
            dataobjs[i].to_capnp(req.objects[i])

        req.send().wait()

    def _wait(self, tasks, dataobjs):
        req = self.service.wait_request()

        req.init("taskIds", len(tasks))
        for i in range(len(tasks)):
            req.taskIds[i].id = tasks[i].id
            req.taskIds[i].sessionId = tasks[i].session.session_id

        req.init("objectIds", len(dataobjs))
        for i in range(len(dataobjs)):
            req.objectIds[i].id = dataobjs[i].id
            req.objectIds[i].sessionId = dataobjs[i].session.session_id

        req.send().wait()

    def _wait_some(self, tasks, dataobjs):
        req = self.service.waitSome_request()

        tasks_dict = {}
        req.init("taskIds", len(tasks))
        for i in range(len(tasks)):
            tasks_dict[tasks[i].id] = tasks[i]
            req.taskIds[i].id = tasks[i].id
            req.taskIds[i].sessionId = tasks[i].session.session_id

        dataobjs_dict = {}
        req.init("objectIds", len(dataobjs))
        for i in range(len(dataobjs)):
            dataobjs_dict[dataobjs[i].id] = dataobjs[i]
            req.objectIds[i].id = dataobjs[i].id
            req.objectIds[i].sessionId = dataobjs[i].session.session_id

        finished = req.send().wait()

        finished_tasks = [tasks_dict[f_task.id] for f_task in finished.finishedTasks]
        finished_dataobjs = [dataobjs_dict[f_dataobj.id] for f_dataobj in finished.finishedObjects]

        return finished_tasks, finished_dataobjs

    def _wait_all(self, session_id):
        req = self.service.wait_request()
        req.init("taskIds", 1)
        req.taskIds[0].id = rpc.common.allTasksId
        req.taskIds[0].sessionId = session_id

        req.init("objectIds", 1)
        req.objectIds[0].id = rpc.common.allDataObjectsId
        req.objectIds[0].sessionId = session_id

        req.send().wait()
