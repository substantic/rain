import capnp
import json

from . import rpc
from ..common import RainException, SessionException, TaskException
from ..common.attributes import ObjectInfo, TaskInfo
from ..common.data_instance import DataInstance
from ..common.ids import governor_id_from_capnp, id_from_capnp, id_to_capnp
from .data import DataObject
from .session import Session
from .task import Task

CLIENT_PROTOCOL_VERSION = 0
FETCH_SIZE = 8 << 20  # 8MB


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
            message.append("Task created at:\n" + task._stack)

        if result.error.debug:
            message.append("Debug:\n" + result.error.debug)
        message = "\n".join(message)
        raise cls(message)
    else:
        raise Exception("Invalid result: {}".format(result))


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
            "governors": [{"governor_id": governor_id_from_capnp(w.governorId),
                           "tasks": [id_from_capnp(t) for t in w.tasks],
                           "objects": [id_from_capnp(o) for o in w.objects],
                           "objects_to_delete": [id_from_capnp(o) for o in w.objectsToDelete],
                           "resources": {"cpus": w.resources.nCpus}}
                          for w in info.governors]
        }

    def _submit(self, tasks, dataobjs):
        req = self._service.submit_request()

        # Serialize tasks        print(tasks, dataobjs)

        req.init("tasks", len(tasks))
        for i in range(len(tasks)):
            req.tasks[i].spec = json.dumps(tasks[i].spec._to_json())

        # Serialize objects
        req.init("objects", len(dataobjs))
        for i in range(len(dataobjs)):
            dataobjs[i]._to_capnp(req.objects[i])

        req.send().wait()

    def _fetch(self, dataobj):
        "Fetch the object data and update its state."
        if not dataobj._keep:
            raise RainException(
                "Can't fetch object {} without keep flag.".format(dataobj))

        if dataobj.state is None:
            raise RainException(
                "Object {} is not submitted.".format(dataobj))

        req = self._service.fetch_request()
        id_to_capnp(dataobj.id, req.id)
        req.offset = 0
        req.size = FETCH_SIZE
        req.includeInfo = True
        result = req.send().wait()
        check_result((dataobj._session,), result.status)

        dataobj._info = ObjectInfo._from_json(json.loads(result.info))

        size = result.transportSize
        offset = len(result.data)
        data = [result.data]

        while offset < size:
            req = self._service.fetch_request()
            id_to_capnp(dataobj.id, req.id)
            req.offset = offset
            req.size = FETCH_SIZE
            req.includeInfo = False
            r = req.send().wait()
            check_result((dataobj._session,), r.status)
            data.append(r.data)
            offset += len(r.data)
        rawdata = b"".join(data)

        return DataInstance(data=rawdata,
                            data_object=dataobj,
                            data_type=dataobj.spec.data_type)

    def _wait(self, tasks, dataobjs):
        req = self._service.wait_request()

        req.init("taskIds", len(tasks))
        sessions = []
        for i in range(len(tasks)):
            task = tasks[i]
            if task.state is None:
                raise RainException("Task {} is not submitted".format(task))
            id_to_capnp(task.id, req.taskIds[i])
            sessions.append(task._session)

        req.init("objectIds", len(dataobjs))
        for i in range(len(dataobjs)):
            id_to_capnp(dataobjs[i].id, req.objectIds[i])
            sessions.append(dataobjs[i]._session)

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
        check_result([o._session for o in dataobjs], result)

    def update(self, items):
        tasks, dataobjects = split_items(items)
        self._get_state(tasks, dataobjects)

    def _get_state(self, tasks, dataobjs):
        req = self._service.getState_request()
        sessions = []
        req.init("taskIds", len(tasks))
        for i in range(len(tasks)):
            id_to_capnp(tasks[i].id, req.taskIds[i])
            sessions.append(tasks[i]._session)

        dataobjs_dict = {}
        req.init("objectIds", len(dataobjs))
        for i in range(len(dataobjs)):
            dataobjs_dict[dataobjs[i].id.id] = dataobjs[i]
            id_to_capnp(dataobjs[i].id, req.objectIds[i])
            sessions.append(dataobjs[i]._session)

        results = req.send().wait()
        check_result(sessions, results.state)

        for task_update, task in zip(results.tasks, tasks):
            task._state = task_update.state
            task._info = TaskInfo._from_json(json.loads(task_update.info))

        for object_update in results.objects:
            dataobj = dataobjs_dict[object_update.id.id]
            dataobj._state = object_update.state
            dataobj._info = ObjectInfo._from_json(json.loads(object_update.info))


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
