
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

    def shutdown(self):
        self.conn.close()