import capnp
from rain.client import rpc
from rain.client.common import RainException
from rain.client.task import Task
from rain.client.data import DataObject
from . import additionals

from .session import Session

CLIENT_PROTOCOL_VERSION = 0


def check_result(result):
    if result.which() == "ok":
        return  # Do nothing
    elif result.which() == "error":
        raise RainException(result.error.message)
    else:
        raise Exception("Invalid result")


class Client:

    def __init__(self, address, port):
        self.submit_id = 0
        self.handles = {}
        self.rpc_client = capnp.TwoPartyClient("{}:{}".format(address, port))

        bootstrap = self.rpc_client.bootstrap().cast_as(rpc.server.ServerBootstrap)
        registration = bootstrap.registerAsClient(CLIENT_PROTOCOL_VERSION)
        self.service = registration.wait().service
        self.datastore = self.service.getDataStore().wait().store

    def new_session(self):
        session_id = self.service.newSession().wait().sessionId
        return Session(self, session_id)

    def get_server_info(self):
        """ Returns basic server info """
        info = self.service.getServerInfo().wait()
        return {
            "workers": [{"n_tasks": w.nTasks, "n_objects" : w.nObjects}
                        for w in info.workers]
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

    def _fetch(self, dataobj):
        if not dataobj._keep:
            raise RainException("Can't fetch object without keep flag.", dataobj)
        req = self.datastore.createReader_request()
        req.id.id = dataobj.id
        req.id.sessionId = dataobj.session.session_id
        req.offset = 0
        result = req.send().wait()
        check_result(result)

        reader = result.reader
        FETCH_SIZE = 2 << 20  # 2MB
        eof = False
        data = []
        while not eof:
            r = reader.read(FETCH_SIZE).wait()
            data.append(r.data)
            eof = r.status == "eof"
        return b"".join(data)

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

        result = req.send().wait()
        check_result(result)

    def _close_session(self, session):
        self.service.closeSession(session.session_id).wait()

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

    def _unkeep(self, dataobjs):
        req = self.service.unkeep_request()

        req.init("objectIds", len(dataobjs))
        for i in range(len(dataobjs)):
            req.objectIds[i].id = dataobjs[i].id
            req.objectIds[i].sessionId = dataobjs[i].session.session_id

        result = req.send().wait()
        check_result(result)

    def update(self, items):
        tasks, dataobjects = split_items(items)
        self._get_state(tasks, dataobjects)

    def _get_state(self, tasks, dataobjs):
        req = self.service.getState_request()

        req.init("taskIds", len(tasks))
        for i in range(len(tasks)):
            req.taskIds[i].id = tasks[i].id
            req.taskIds[i].sessionId = tasks[i].session.session_id

        dataobjs_dict = {}
        req.init("objectIds", len(dataobjs))
        for i in range(len(dataobjs)):
            dataobjs_dict[dataobjs[i].id] = dataobjs[i]
            req.objectIds[i].id = dataobjs[i].id
            req.objectIds[i].sessionId = dataobjs[i].session.session_id

        results = req.send().wait()
        check_result(results.state)

        for task_update, task in zip(results.tasks, tasks):
            task.state = task_update.state
            new_additionals = additionals.additionals_from_capnp(task_update.additionals)
            if task.additionals is None:
                task.additionals = new_additionals
            else:
                task.additionals.update(new_additionals)

        for object_update in results.objects:
            dataobj = dataobjs_dict[object_update.id.id]
            dataobj.state = object_update.state
            dataobj.size = object_update.size


def split_items(items):
    """Split items into 'tasks' and 'dataobjects'
    Throws an error if an item is not task nor object"""
    tasks = []
    dataobjects = []
    for item in items:
        if isinstance(item, Task):
            tasks.append(item)
        elif isinstance(item, DataObject):
            dataobjects.append(item)
        else:
            raise RainException("'{}' is not tasks nor dataobject".format(item))
    return tasks, dataobjects
