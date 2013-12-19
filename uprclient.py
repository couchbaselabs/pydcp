
import constants
import logging

from conn import Connection
from constants import *
from op import *

class UprClient():
    def __init__(self, host='127.0.0.1', port=11211):
        self.conn = Connection(host, port)
        self.conn.connect()

    def set_proxy(self, client):
        self.conn.proxy = client.conn.socket

    def open_consumer(self, name):
        op = OpenConnection(FLAG_OPEN_CONSUMER, name)
        self.conn.queue_operation(op)
        return op

    def open_producer(self, name):
        op = OpenConnection(FLAG_OPEN_PRODUCER, name)
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

    def stream_req(self, vb, flags, start_seqno, end_seqno, vb_uuid, hi_seqno):
        op = StreamRequest(vb, flags, start_seqno, end_seqno, vb_uuid, hi_seqno)
        self.conn.queue_operation(op)
        return op

    def shutdown(self):
        self.conn.close()

