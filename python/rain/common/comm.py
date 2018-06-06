
import struct

import cbor


class SocketWrapper:

    header = struct.Struct("<I")
    header_size = 4
    read_buffer_size = 256 * 1024

    def __init__(self, socket):
        self.socket = socket
        self._buffer = bytes()

    def close(self):
        self.socket.close()

    def send_message(self, message):
        msg = cbor.dumps(message)
        data = self.header.pack(len(msg)) + msg
        self.socket.sendall(data)

    def receive_message(self):
        header_size = self.header_size
        while True:
            size = len(self._buffer)
            if size >= header_size:
                msg_size = self.header.unpack(self._buffer[:header_size])[0] + header_size
                if size >= msg_size:
                    message = self._buffer[header_size:msg_size]
                    self._buffer = self._buffer[msg_size:]
                    return cbor.loads(message)

            new_data = self.socket.recv(self.read_buffer_size)
            if not new_data:
                raise Exception("Connection to server lost")
            self._buffer += new_data
