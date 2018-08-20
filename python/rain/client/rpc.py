import asyncio

import cbor
import websockets

ALL_TASKS_ID = -2
ALL_DATA_OBJECTS_ID = -2


class TaskState(object):
    Finished = "Finished"
    NotAssigned = "NotAssigned"


class DataObjectState(object):
    Finished = "Finished"
    Unfinished = "Unfinished"


def block(fut, loop=None):
    if not loop:
        loop = asyncio.get_event_loop()

    return loop.run_until_complete(fut)


class WsCommunicator(object):
    def __init__(self, address, port):
        self.id = 0
        self.ws = block(websockets.connect("ws://{}:{}".format(address, port),
                                           subprotocols=['rain-ws'],
                                           max_size=None))

    def request(self, method, data=None, allow_failure=False):
        if not self.ws.open:
            if allow_failure:
                return
            else:
                raise Exception("Client is not connected")

        if data is None:
            data = {}

        id = self.id
        self.id += 1

        @asyncio.coroutine
        def receive():
            try:
                msg = yield from self.ws.recv()
                msg = self.deserialize(msg)

                if msg["id"] == id:
                    return msg["data"][1]
                else:
                    print("Error: invalid id received, sent: {}, received: {}"
                          .format(id, msg["id"]))
            except websockets.ConnectionClosed as e:
                if not allow_failure:
                    raise e

        msg = {
            "id": id,
            "data": [method, data]
        }

        block(self.ws.send(self.serialize(msg)))
        return block(receive())

    def serialize(self, msg):
        return cbor.dumps(msg)

    def deserialize(self, msg):
        return cbor.loads(msg)
