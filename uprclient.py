
import constants
import logging

from conn import Connection
from constants import *
from op import *

class UprClient():
    def __init__(self, host='127.0.0.1', port=11211):
        self.conn = Connection(host, port)
        self.conn.connect()

    def sasl_auth_plain(self, username, password):
        op = SaslPlain(username, password)
        self.conn.queue_operation(op)
        return op

    def set_proxy(self, client):
        self.conn.proxy = client.conn.socket

    def open_consumer(self, name):
        return self.open_generic(FLAG_OPEN_CONSUMER, name)

    def open_producer(self, name):
        return self.open_generic(FLAG_OPEN_PRODUCER, name)

    def open_notifier(self, name):
        return self.open_generic(FLAG_OPEN_NOTIFIER, name)

    def open_generic(self, flag, name, seqno = 0, extras = None):
        op = OpenConnection(flag, name, seqno, extras)
        self.conn.queue_operation(op)
        return op

    def add_stream(self, vbucket, flags):
        op = AddStream(vbucket, flags)
        self.conn.queue_operation(op)
        return op

    def close_stream(self, vbucket):
        op = CloseStream(vbucket)
        self.conn.queue_operation(op)
        return op

    def get_failover_log(self, vbucket):
        op = GetFailoverLog(vbucket)
        self.conn.queue_operation(op)
        return op

    def stream_req(self, vb, flags = 0, start_seqno = 0, end_seqno = 0, vb_uuid = 0,
                   snap_start_seqno = 0, snap_end_seqno = 0):
        if snap_start_seqno is None:
                # use None for snapstart to auto assign snap values to start_seqno
                snap_start_seqno = snap_end_seqno = start_seqno

        op = StreamRequest(vb, flags, start_seqno, end_seqno, vb_uuid,
                           snap_start_seqno, snap_end_seqno)
        self.conn.queue_operation(op)
        return op

    def shutdown(self):
        self.conn.close()

    def noop(self):
        op = Noop()
        self.conn.queue_operation(op)
        return op

    def ack(self, nbytes):
        op = Ack(nbytes)
        self.conn.queue_operation(op)
        return op

    def flow_control(self, buffer_size):
        op = FlowControl(buffer_size)
        self.conn.queue_operation(op)
        return op
