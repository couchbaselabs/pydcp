
import binascii
import constants
import logging
import Queue
import struct

from constants import *

class Operation():
    opaque_counter = 0xFFFF0000

    def __init__(self, opcode, data_type, vbucket, cas, key, value):
        Operation.opaque_counter = Operation.opaque_counter + 1
        self.opcode = opcode
        self.data_type = data_type
        self.vbucket = vbucket
        self.opaque = Operation.opaque_counter
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
        return self.responses.qsize() > 0 or not self.ended

    def next_response(self):
        if self.ended and self.responses.qsize() == 0:
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

        opaque = None
        if extlen == 4:
            opaque = struct.unpack(">I", body[0:4])
        self.responses.put({ 'opcode'        : opcode,
                             'status'        : status,
                             'stream_opaque' : opaque,
                             'extlen'        : extlen,
                             'value'         : body[4:] })
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
        if opcode == CMD_STREAM_REQ:
            logging.info("(Stream Request) Received OK")
            assert cas == 0
            assert keylen == 0
            assert extlen == 0

            result = { 'opcode' : opcode,
                       'status' : status }

            if status == SUCCESS:
                assert (len(body) % 8) == 0
                result['failover_log'] = []

                pos = 0
                bodylen = len(body)
                while bodylen > pos:
                    vb_uuid, seqno = struct.unpack(">QQ", body[pos:pos+16])
                    result['failover_log'].append((vb_uuid, seqno))
                    pos += 16
            else:
                result['err_msg'] = body

            self.responses.put(result)
            if status != SUCCESS:
                return True
        elif opcode == CMD_STREAM_END:
            logging.info("(Stream Request) Received stream end")
            assert cas == 0
            assert keylen == 0
            assert extlen == 4
            flags = struct.unpack(">I", body[0:4])[0]
            self.responses.put({ 'opcode' : opcode,
                                 'vbucket' : status,
                                 'flags'  : flags })
            self.ended = True
            return True
        elif opcode == CMD_MUTATION:
            logging.info("(Stream Request) Received mutation")
            by_seqno, rev_seqno, flags, exp, lock_time, ext_meta_len = \
                struct.unpack(">QQIIIH", body[0:30])
            key = body[30:30+keylen]
            value = body[30+keylen:]
            self.responses.put({ 'opcode'     : opcode,
                                 'vbucket'     : status,
                                 'by_seqno'   : by_seqno,
                                 'rev_seqno'  : rev_seqno,
                                 'flags'      : flags,
                                 'expiration' : exp,
                                 'lock_time'  : lock_time,
                                 'key'        : key,
                                 'value'      : value })
        elif opcode == CMD_DELETION:
            logging.info("(Stream Request) Received deletion")
            by_seqno, rev_seqno, ext_meta_len = \
                struct.unpack(">QQH", body[0:18])
            key = body[18:18+keylen]
            self.responses.put({ 'opcode'     : opcode,
                                 'vbucket'     : status,
                                 'by_seqno'   : by_seqno,
                                 'rev_seqno'  : rev_seqno,
                                 'key'        : key })
        elif opcode == CMD_SNAPSHOT_MARKER:
            logging.info("(Stream Request) Received snapshot marker")
            self.responses.put({ 'opcode'     : opcode,
                                 'vbucket'     : status })
        else:
            logging.error("(Stream Request) Unknown response: %s" % opcode)

        return False

    def _get_extras(self):
        return struct.pack(">IIQQQQ", self.flags, 0, self.start_seqno,
                           self.end_seqno, self.vb_uuid, self.high_seqno)

############################ Memcached Operations ############################

class Stats(Operation):
    def __init__(self, type):
        Operation.__init__(self, CMD_STATS, 0, 0, 0, type, '')
        self.stats = {}

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert cas == 0
        assert extlen == 0
        self.stats[body[0:keylen]] = body[keylen:]

        if keylen > 0:
            return False

        self.end = True
        self.responses.put({ 'opcode': opcode,
                             'status': status,
                             'value' : self.stats })
        return True

    def _get_extras(self):
        return ''

class Set(Operation):
    def __init__(self, key, value, vbucket, flags, exp):
        Operation.__init__(self, CMD_SET, 0, vbucket, 0, key, value)
        self.flags = flags
        self.exp = exp

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert extlen == 0
        assert keylen == 0

        self.end = True
        self.responses.put({ 'opcode': opcode,
                             'status': status,
                             'cas'   : cas })
        return True

    def _get_extras(self):
        return struct.pack(">II", self.flags, self.exp)

class Delete(Operation):
    def __init__(self, key, vbucket):
        Operation.__init__(self, CMD_DELETE, 0, vbucket, 0, key, '')

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert extlen == 0
        assert keylen == 0

        self.end = True
        self.responses.put({ 'opcode': opcode,
                             'status': status,
                             'cas'   : cas })
        return True

    def _get_extras(self):
        return ''

class Flush(Operation):
    def __init__(self):
        Operation.__init__(self, CMD_FLUSH, 0, 0, 0, '', '')

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert extlen == 0
        assert keylen == 0

        self.end = True
        self.responses.put({ 'opcode': opcode,
                             'status': status })
        return True

    def _get_extras(self):
        return struct.pack(">I", 0)

class StopPersistence(Operation):
    def __init__(self):
        Operation.__init__(self, CMD_STOP_PERSISTENCE, 0, 0, 0, '', '')

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert extlen == 0
        assert keylen == 0

        self.end = True
        self.responses.put({ 'opcode': opcode,
                             'status': status })
        return True

    def _get_extras(self):
        return ''

class StartPersistence(Operation):
    def __init__(self):
        Operation.__init__(self, CMD_START_PERSISTENCE, 0, 0, 0, '', '')

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert extlen == 0
        assert keylen == 0

        self.end = True
        self.responses.put({ 'opcode': opcode,
                             'status': status })
        return True

    def _get_extras(self):
        return ''
