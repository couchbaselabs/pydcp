
import logging
import select
import socket
import threading
import time

from constants import *
from op import *

class Connection(threading.Thread):
    def __init__(self, host='127.0.0.1', port=11211):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.socket = None
        self.running = False
        self.ops = []

    def connect(self):
        logging.info("Connecting to %s:%d" % (self.host, self.port))
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.start()
        except Exception, e:
            self.socket = None
            logging.error("Could not connect: %s" % e)

    def close(self, force = False):
        if not force:
            secs = 0
            while len(self.ops) > 0 and secs < 5:
                time.sleep(1)
                logging.info("Waiting for %d ops" % len(self.ops))
                secs = secs + 1
        logging.info("Closing connection to %s:%d" % (self.host, self.port))
        if self.socket:
            self.running = False
            self.join()
            self.socket.close()

    def queue_operation(self, op):
        if not self.running:
            op.network_error()
            return
        for o in self.ops:
            assert op.opaque != o.opaque
        self.ops.append(op)
        self.socket.send(op.bytes())

    def run(self):
        self.running = True

        bytes_read = ''
        rd_timeout = 1
        desc = [self.socket]
        while self.running:
            readers, writers, errors = select.select(desc, [], [], rd_timeout)
            rd_timeout = .25

            for reader in readers:
                data = reader.recv(1024)
                logging.debug("Read %d bytes off the wire" % len(data))
                if len(data) == 0:
                    self._connection_lost()
                bytes_read += data

            while len(bytes_read) >= HEADER_LEN:
                magic, opcode, keylen, extlen, dt, status, bodylen, opaque, cas=\
                    struct.unpack(PKT_HEADER_FMT, bytes_read[0:HEADER_LEN])
                unaligned_opaque = struct.unpack("=I", bytes_read[12:16])[0]

                if len(bytes_read) < (HEADER_LEN+bodylen):
                    break

                rd_timeout = 0
                body = bytes_read[HEADER_LEN:HEADER_LEN+bodylen]
                bytes_read = bytes_read[HEADER_LEN+bodylen:]

                if not self.ops:
                    self._handle_random_opaque(opcode, status, opaque)

                for op in self.ops:
                    if op.opaque == opaque:
                        rm = op.add_response(opcode, keylen, extlen,
                                                 status, cas, body)
                        if rm:
                            self.ops.remove(op)
                        break
                    else:
                        self._handle_random_opaque(opcode, status, unaligned_opaque)

    def _handle_random_opaque(self, opcode, vbucket, opaque):
        if opcode == CMD_STREAM_REQ:
            logging.info("Recieve stream request")
            resp = struct.pack(PKT_HEADER_FMT, RES_MAGIC, opcode,
                               0, 0, 0, vbucket, 0, opaque, 0)
            logging.info("Sending stream response")
            self.socket.send(resp)
        logging.debug('No matching op for resp (opcode %d)(opaque %d)'
                      % (opcode, opaque))

    def _connection_lost(self):
        self.running = False
        for op in self.ops:
            op.network_error()
            self.ops.remove(op)
        logging.warning("Socket closed unexpectedly")
