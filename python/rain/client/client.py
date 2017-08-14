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
        self.datastore = self.service.getDataStore().wait()

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
