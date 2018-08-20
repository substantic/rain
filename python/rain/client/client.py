import json

from .rpc import WsCommunicator, ALL_TASKS_ID
from .data import DataObject
from .session import Session
from .task import Task
from ..common import RainException, SessionException, TaskException
from ..common.attributes import ObjectInfo, TaskInfo
from ..common.data_instance import DataInstance
from ..common.ids import ID

CLIENT_PROTOCOL_VERSION = 1
FETCH_SIZE = 8 << 20  # 8MB


def check_result(sessions, result):
    if result is None:
        return

    status = result if isinstance(result, str) else result["status"]

    if status == "Ok":
        return  # Do nothing
    elif isinstance(status, list) and status[0] == "Error":
        data = status[1]

        task_id = ID._from_json(data["task"])
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

        message.append("Message: " + data["message"])

        if task:
            message.append("Task created at:\n" + task._stack)

        if data["debug"]:
            message.append("Debug:\n" + data["debug"])
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
        self._rpc_client = WsCommunicator(address, port)
        self._rpc_client.request("RegisterClient", {
            "version": CLIENT_PROTOCOL_VERSION
        })

    def new_session(self, name="Unnamed Session", default=False):
        """
        Creates a new session.

        Note the session is destroyed server-side when the client disconnects.

        Returns:
            :class:`Session`: A new session
        """
        spec = json.dumps({"name": str(name)})
        session_id = self._rpc_client.request("NewSession", {
            "spec": spec
        })["session_id"]
        return Session(self, session_id, default)

    def get_server_info(self):
        """
        Returns basic server info. Unstable.

        Returns:
            dict: A JSON-like dictionary.
        """
        info = self._rpc_client.request("GetServerInfo")["governors"]
        return {
            "governors": [{
                "governor_id": g["governor_id"],
                "tasks": [ID._from_json(id) for id in g["tasks"]],
                "objects": [ID._from_json(id) for id in g["objects"]],
                "objects_to_delete": [ID._from_json(id) for id in g["objects_to_delete"]],
                "resources": g["resources"],
            } for g in info]
        }

    def _submit(self, tasks, dataobjs):
        # Serialize tasks
        tasks_data = [{
            "spec": json.dumps(t.spec._to_json())
        } for t in tasks]

        # Serialize objects
        objects_data = [d._to_json() for d in dataobjs]

        return self._rpc_client.request("Submit", {
            "tasks": tasks_data,
            "objects": objects_data
        })

    def _fetch(self, dataobj):
        "Fetch the object data and update its state."
        if not dataobj._keep:
            raise RainException(
                "Can't fetch object {} without keep flag.".format(dataobj))

        if dataobj.state is None:
            raise RainException(
                "Object {} is not submitted.".format(dataobj))

        msg = {
            "id": dataobj.id,
            "include_info": True,
            "offset": 0,
            "size": FETCH_SIZE
        }

        result = self._rpc_client.request("Fetch", msg)
        check_result((dataobj._session,), result)

        dataobj._info = ObjectInfo._from_json(json.loads(result["info"]))

        size = result["transport_size"]
        offset = len(result["data"])
        data = [bytearray(result["data"])]

        while offset < size:
            msg = {
                "id": dataobj.id,
                "include_info": False,
                "offset": offset,
                "size": FETCH_SIZE
            }

            result = self._rpc_client.request("Fetch", msg)
            check_result((dataobj._session,), result["status"])
            data.append(result["data"])
            offset += len(result["data"])
        rawdata = b"".join(data)

        return DataInstance(data=rawdata,
                            data_object=dataobj,
                            data_type=dataobj.spec.data_type)

    def _wait(self, tasks, dataobjs):
        sessions = []
        for i in range(len(tasks)):
            task = tasks[i]
            if task.state is None:
                raise RainException("Task {} is not submitted".format(task))
            sessions.append(task._session)

        msg = {
            "task_ids": [t.id for t in tasks],
            "object_ids": [d.id for d in dataobjs]
        }

        for i in range(len(dataobjs)):
            sessions.append(dataobjs[i]._session)

        result = self._rpc_client.request("Wait", msg)
        check_result(sessions, result)

    def _close_session(self, session):
        return self._rpc_client.request("CloseSession", {
            "session_id": session.session_id
        }, allow_failure=True)

    def _wait_some(self, tasks, dataobjs):
        tasks_dict = {}
        for i in range(len(tasks)):
            tasks_dict[tasks[i].id] = tasks[i]

        dataobjs_dict = {}
        for i in range(len(dataobjs)):
            dataobjs_dict[dataobjs[i].id] = dataobjs[i]

        msg = {
            "task_ids": [t.id for t in tasks],
            "object_ids": [d.id for d in dataobjs]
        }

        finished = self._rpc_client.request("WaitSome", msg)
        finished_tasks = [tasks_dict[ID._from_json(f_task["id"])]
                          for f_task in finished["finished_tasks"]]
        finished_dataobjs = [dataobjs_dict[ID._from_json(f_dataobj["id"])]
                             for f_dataobj in finished["finished_objects"]]

        return finished_tasks, finished_dataobjs

    def _wait_all(self, session):
        msg = {
            "task_ids": [ID(session_id=session.session_id, id=ALL_TASKS_ID)],
            "object_ids": []
        }

        result = self._rpc_client.request("Wait", msg)
        check_result((session,), result)

    def _unkeep(self, dataobjs):
        result = self._rpc_client.request("Unkeep", {
            "object_ids": [d.id for d in dataobjs]
        }, allow_failure=True)
        check_result([o._session for o in dataobjs], result)

    def update(self, items):
        tasks, dataobjects = split_items(items)
        self._get_state(tasks, dataobjects)

    def _get_state(self, tasks, dataobjs):
        sessions = []
        for i in range(len(tasks)):
            sessions.append(tasks[i]._session)

        dataobjs_dict = {}
        for i in range(len(dataobjs)):
            dataobjs_dict[dataobjs[i].id.id] = dataobjs[i]
            sessions.append(dataobjs[i]._session)

        msg = {
            "task_ids": [t.id for t in tasks],
            "object_ids": [d.id for d in dataobjs]
        }

        results = self._rpc_client.request("GetState", msg)["update"]
        check_result(sessions, results)

        for task_update, task in zip(results["tasks"], tasks):
            task._state = task_update["state"]
            task._info = TaskInfo._from_json(json.loads(task_update["info"]))

        for object_update in results["objects"]:
            dataobj = dataobjs_dict[ID._from_json(object_update["id"]).id]
            dataobj._state = object_update["state"]
            dataobj._info = ObjectInfo._from_json(json.loads(object_update["info"]))


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
