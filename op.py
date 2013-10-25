
import binascii
import constants
import Queue
import struct

from constants import *

class Operation():
    opaque_counter = 0

    def __init__(self, opcode, data_type, vbucket, cas, key, value):
        Operation.opaque = Operation.opaque_counter + 1
        self.opcode = opcode
        self.data_type = data_type
        self.vbucket = vbucket
        self.opaque = Operation.opaque
        self.cas = cas
        self.key = key
        self.value = value
        self.responses = Queue.Queue()
        self.ended = False

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        raise NotImplementedError("Subclass must implement abstract method")

    def network_error(self):
        self.responses.put({ 'status' : ERR_ECLIENT })
        self.ended = True

    def has_response(self):
        return self.responses.qsize() > 0

    def next_response(self):
        if self.ended:
            return None
        return self.responses.get(True)

    def bytes(self):
        extras = self._get_extras()
        bodylen = len(self.key) + len(extras) + len(self.value)
        header = struct.pack(PKT_HEADER_FMT, REQ_MAGIC, self.opcode,
                             len(self.key), len(extras), self.data_type,
                             self.vbucket, bodylen, self.opaque, self.cas)
        return header + extras + self.key + self.value

    def _get_extras(self):
        raise NotImplementedError("Subclass must implement abstract method")

    def __str__(self):
        ret = ''
        raw = binascii.hexlify(self.bytes())
        for i in range(len(raw))[0::2]:
            ret += raw[i] + raw[i+1] + ' '
            if (i+2) % 8 == 0:
                ret += '\n'
        return ret

class OpenConnection(Operation):
    def __init__(self, flags, name):
        Operation.__init__(self, CMD_OPEN, 0, 0, 0, name, '')
        self.flags = flags

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert cas == 0
        assert keylen == 0
        assert extlen == 0
        self.responses.put({ 'opcode' : opcode,
                             'status' : status,
                             'value'  : body })
        self.ended = True
        return True

    def _get_extras(self):
        return struct.pack(">II", 0, self.flags)

class AddStream(Operation):
    def __init__(self, vbucket, flags):
        Operation.__init__(self, CMD_ADD_STREAM, 0, vbucket, 0, '', '')
        self.flags = flags

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert cas == 0
        assert keylen == 0
        assert extlen == 4
        opaque = struct.unpack(">I", body[0:4])
        self.responses.put({ 'opcode' : opcode,
                             'status' : status,
                             'opaque' : opaque,
                             'value'  : body[4:] })
        self.ended = True
        return True

    def _get_extras(self):
        return struct.pack(">I", self.flags)

class CloseStream(Operation):
    def __init__(self, vbucket):
        Operation.__init__(self, CMD_CLOSE_STREAM, 0, vbucket, 0, '', '')

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert cas == 0
        assert keylen == 0
        assert extlen == 0
        self.responses.put({ 'opcode' : opcode,
                             'status' : status,
                             'value'  : body })
        self.end = True
        return True

    def _get_extras(self):
        return ''

class GetFailoverLog(Operation):
    def __init__(self, vbucket):
        Operation.__init__(self, CMD_GET_FAILOVER_LOG, 0, vbucket, 0, '', '')

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert cas == 0
        assert keylen == 0
        assert extlen == 0
        self.responses.put({ 'opcode' : opcode,
                             'status' : status,
                             'value'  : body })
        self.end = True
        return True

    def _get_extras(self):
        return ''

class StreamRequest(Operation):
    def __init__(self, vb, flags, start_seqno, end_seqno, vb_uuid, high_seqno):
        Operation.__init__(self, CMD_STREAM_REQ, 0, vb, 0, '', '')
        self.flags = flags
        self.start_seqno = start_seqno
        self.end_seqno = end_seqno
        self.vb_uuid = vb_uuid
        self.high_seqno = high_seqno

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert cas == 0
        assert keylen == 0
        assert extlen == 0
        self.responses.put({ 'opcode' : opcode,
                             'status' : status,
                             'value'  : body })
        self.end = True
        return True

    def _get_extras(self):
        return struct.pack(">IIQQQQ", self.flags, 0, self.start_seqno,
                           self.end_seqno, self.vb_uuid, self.high_seqno)



