
import logging

from conn import Connection
from constants import *
from op import *

class McdClient():
    def __init__(self, host='127.0.0.1', port=11211):
        self.conn = Connection(host, port)
        self.conn.connect()

    def stats(self, type = ''):
        op = Stats(type)
        self.conn.queue_operation(op)
        return op

    def set(self, key, value, vbucket, flags, exp):
        op = Set(key, value, vbucket, flags, exp)
        self.conn.queue_operation(op)
        return op

    def delete(self, key, vbucket):
        op = Delete(key, vbucket)
        self.conn.queue_operation(op)
        return op

    def flush(self):
        op = Flush()
        self.conn.queue_operation(op)
        return op

    def start_persistence(self):
        op = StartPersistence()
        self.conn.queue_operation(op)
        return op

    def stop_persistence(self):
        op = StopPersistence()
        self.conn.queue_operation(op)
        return op

    def shutdown(self):
        self.conn.close()