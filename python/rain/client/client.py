import capnp
from rain.client import rpc
from rain.common import RainException, SessionException, TaskException
from rain.client.task import Task
from rain.client.data import DataObject
from ..common import attributes, DataInstance, DataType
from ..common.ids import id_from_capnp, id_to_capnp, worker_id_from_capnp
from .session import Session

CLIENT_PROTOCOL_VERSION = 0


def check_result(sessions, result):
    if result.which() == "ok":
        return  # Do nothing
    elif result.which() == "error":
        task_id = id_from_capnp(result.error.task)
        message = []

        if task_id.session_id == -1:
            cls = SessionException
            task = None
        else:
            cls = TaskException

            for session in sessions:
                if session.session_id == task_id.session_id:
                    break
            else:
                raise Exception("Unknown session {} failed. Internal error".format(task_id))

            for task in session._submitted_tasks:
                if task.id.id == task_id.id:
                    break
            else:
                raise Exception("Unknown task {} failed. Internal error".format(task_id))

            message.append("Task {} failed".format(task))

        message.append("Message: " + result.error.message)

        if task:
            message.append("Task created at:\n" + task.stack)

        if result.error.debug:
            message.append("Debug:\n" + result.error.debug)
        message = "\n".join(message)
        raise cls(message)
    else:
        raise Exception("Invalid result")


class Client:
    """
    A client connection object. Can hold multiple
    :py:class:`Session`\ s.
    """

    def __init__(self, address, port):
        self._rpc_client = capnp.TwoPartyClient("{}:{}".format(address, port))

        bootstrap = self._rpc_client.bootstrap().cast_as(
            rpc.server.ServerBootstrap)
        registration = bootstrap.registerAsClient(CLIENT_PROTOCOL_VERSION)
        self._service = registration.wait().service
        self._datastore = self._service.getDataStore().wait().store

    def new_session(self):
        """
        Creates a new session.

        Note the session is destroyed server-side when the client disconnects.

        Returns:
            :class:`Session`: A new session
        """
        session_id = self._service.newSession().wait().sessionId
        return Session(self, session_id)

    def get_server_info(self):
        """
        Returns basic server info. Unstable.

        Returns:
            dict: A JSON-like dictionary.
        """
        info = self._service.getServerInfo().wait()
        return {
            "workers": [{"worker_id": worker_id_from_capnp(w.workerId),
                         "tasks": [id_from_capnp(t) for t in w.tasks],
                         "objects": [id_from_capnp(o) for o in w.objects],
                         "objects_to_delete": [id_from_capnp(o) for o in w.objectsToDelete],
                         "resources": {"cpus": w.resources.nCpus}}
                        for w in info.workers]
        }

    def _submit(self, tasks, dataobjs):
        req = self._service.submit_request()

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
        "Fetch the object data and update its state."
        if not dataobj._keep:
            raise RainException(
                "Can't fetch object {} without keep flag.".format(dataobj))

        if dataobj.state is None:
            raise RainException(
                "Object {} is not submitted.".format(dataobj))

        req = self._datastore.createReader_request()
        id_to_capnp(dataobj.id, req.id)
        req.offset = 0
        result = req.send().wait()
        check_result((dataobj.session,), result)

        reader = result.reader
        FETCH_SIZE = 2 << 20  # 2MB
        eof = False
        data = []
        while not eof:
            r = reader.read(FETCH_SIZE).wait()
            data.append(r.data)
            eof = r.status == "eof"
        bytedata = b"".join(data)
        self._get_state((), (dataobj, ))
        return DataInstance(data=bytedata,
                            data_object=dataobj,
                            data_type=DataType.from_capnp(result.dataType))

    def _wait(self, tasks, dataobjs):
        req = self._service.wait_request()

        req.init("taskIds", len(tasks))
        sessions = []
        for i in range(len(tasks)):
            task = tasks[i]
            if task.state is None:
                raise RainException("Task {} is not submitted".format(task))
            id_to_capnp(task.id, req.taskIds[i])
            sessions.append(task.session)

        req.init("objectIds", len(dataobjs))
        for i in range(len(dataobjs)):
            id_to_capnp(dataobjs[i].id, req.objectIds[i])
            sessions.append(dataobjs[i].session)

        result = req.send().wait()
        check_result(sessions, result)

    def _close_session(self, session):
        self._service.closeSession(session.session_id).wait()

    def _wait_some(self, tasks, dataobjs):
        req = self._service.waitSome_request()

        tasks_dict = {}
        req.init("taskIds", len(tasks))
        for i in range(len(tasks)):
            tasks_dict[tasks[i].id] = tasks[i]
            id_to_capnp(tasks[i].id, req.taskIds[i])

        dataobjs_dict = {}
        req.init("objectIds", len(dataobjs))
        for i in range(len(dataobjs)):
            dataobjs_dict[dataobjs[i].id] = dataobjs[i]
            id_to_capnp(dataobjs[i].id, req.objectIds[i])

        finished = req.send().wait()
        finished_tasks = [tasks_dict[f_task.id]
                          for f_task in finished.finishedTasks]
        finished_dataobjs = [dataobjs_dict[f_dataobj.id]
                             for f_dataobj in finished.finishedObjects]

        return finished_tasks, finished_dataobjs

    def _wait_all(self, session):
        req = self._service.wait_request()
        req.init("taskIds", 1)
        req.taskIds[0].id = rpc.common.allTasksId
        req.taskIds[0].sessionId = session.session_id
        result = req.send().wait()
        check_result((session,), result)

    def _unkeep(self, dataobjs):
        req = self._service.unkeep_request()

        req.init("objectIds", len(dataobjs))
        for i in range(len(dataobjs)):
            id_to_capnp(dataobjs[i].id, req.objectIds[i])

        result = req.send().wait()
        check_result([o.session for o in dataobjs], result)

    def update(self, items):
        tasks, dataobjects = split_items(items)
        self._get_state(tasks, dataobjects)

    def _get_state(self, tasks, dataobjs):
        req = self._service.getState_request()
        sessions = []
        req.init("taskIds", len(tasks))
        for i in range(len(tasks)):
            id_to_capnp(tasks[i].id, req.taskIds[i])
            sessions.append(tasks[i].session)

        dataobjs_dict = {}
        req.init("objectIds", len(dataobjs))
        for i in range(len(dataobjs)):
            dataobjs_dict[dataobjs[i].id.id] = dataobjs[i]
            id_to_capnp(dataobjs[i].id, req.objectIds[i])
            sessions.append(dataobjs[i].session)

        results = req.send().wait()
        check_result(sessions, results.state)

        for task_update, task in zip(results.tasks, tasks):
            task.state = task_update.state
            new_attributes = attributes.attributes_from_capnp(
                task_update.attributes)
            task.attributes.update(new_attributes)

        for object_update in results.objects:
            dataobj = dataobjs_dict[object_update.id.id]
            dataobj.state = object_update.state
            dataobj.size = object_update.size
            dataobj.attributes = attributes.attributes_from_capnp(
                object_update.attributes)


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
            raise RainException(
                "'{}' is not tasks nor dataobject".format(item))
    return tasks, dataobjects
