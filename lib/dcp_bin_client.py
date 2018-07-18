import random
from mc_bin_client import MemcachedClient
from memcacheConstants import *
import Queue
import time

MAX_SEQNO = 0xFFFFFFFFFFFFFFFF


class DcpClient(MemcachedClient):
    """ DcpClient implements dcp protocol using mc_bin_client as base
        for sending and receiving commands """

    def __init__(self, host='127.0.0.1', port=11210, timeout=30, do_auth=True):
        super(DcpClient, self).__init__(host, port, timeout, do_auth=do_auth)

        # recv timeout
        self.timeout = timeout

        # map of dcpstreams.  key = vbucket, val = DcpStream
        self.streams = {}

        # inflight ops.  key = opaque, val = Operation
        self.ops = {}

        self.dead = False

        # open_producer defines if collections are being streamed
        self.collections = False

        # Option to print out opcodes received
        self.__opcode_dump = False

    def _open(self, op):
        return self._handle_op(op)

    def close(self):
        super(DcpClient, self).close()
        self.dead = True

    def open_consumer(self, name):
        """ opens an dcp consumer connection """

        op = OpenConsumer(name)
        return self._open(op)

    def open_producer(self, name, xattr=False, delete_times=False, collections=False, json=''):
        """ opens an dcp producer connection """
        self.collections = collections
        self.delete_times = delete_times
        op = OpenProducer(name, xattr, delete_times, collections, json)
        return self._open(op)

    def open_notifier(self, name):
        """ opens an dcp notifier connection """

        op = OpenNotifier(name)
        return self._open(op)

    def get_failover_log(self, vbucket):
        """ get dcp failover log """

        op = GetFailoverLog(vbucket)
        return self._handle_op(op)

    def flow_control(self, buffer_size):
        """ sent to notify producer how much data a client is able to receive
            while streaming mutations"""

        op = FlowControl(buffer_size)
        return self._handle_op(op)

    def general_control(self, key, value):
        """ sent to notify producer how much data a client is able to receive
            while streaming mutations"""

        op = GeneralControl(key, value)
        return self._handle_op(op)

    def ack(self, nbytes):
        """ sent to notify producer number of bytes client has received"""

        op = Ack(nbytes)
        return self._handle_op(op, 1)

    def quit(self):
        """ send quit command to mc - when response is recieved quit reader """
        op = Quit()
        r = {'opcode': op.opcode}
        if not self.dead:
            r = self._handle_op(op)
            if r['status'] == 0:
                self.dead = True
        else:
            r['status'] = 0xff

        return r

    def add_stream(self, vbucket, takeover=0):
        """ sent to dcp-consumer to add stream on a particular vbucket.
            the takeover flag is there for completeness and is used by
            ns_server during vbucket move """

        op = AddStream(vbucket, takeover)
        return self._handle_op(op)

    def close_stream(self, vbucket):
        """ sent either to producer or consumer to close stream openned by
            this clients connection on specified vbucket """

        op = CloseStream(vbucket)
        return self._handle_op(op)

    def stream_req(self, vbucket, takeover, start_seqno, end_seqno,
                   vb_uuid, snap_start=None, snap_end=None):
        """" sent to dcp-producer to stream mutations from
             a particular vbucket.

             upon sucessful stream request an DcpStream object is created
             that can be used by the client to receive mutations.

             vbucket = vbucket number to strem mutations from
             takeover = specify takeover flag 0|1
             start_seqno = seqno to begin streaming
             end_seqno = seqno to specify end of stream
             vb_uuid = vbucket uuid as specified in failoverlog
             snapt_start = start seqno of snapshot
             snap_end = end seqno of snapshot """

        op = StreamRequest(vbucket, takeover, start_seqno, end_seqno,
                           vb_uuid, snap_start, snap_end, delete_times=self.delete_times, collections=self.collections)

        response = self._handle_op(op)

        def __generator(response):

            yield response
            last_by_seqno = 0

            while True:

                if not op.queue.empty():
                    response = op.queue.get()
                else:
                    response = self.recv_op(op)
                yield response

                if response and response['opcode'] == CMD_STREAM_END:
                    break

        # start generator and pass to dcpStream class
        generator = __generator(response)
        return DcpStream(generator, vbucket)

    def get_stream(self, vbucket):
        """ for use by external clients to get stream
            associated with a particular vbucket """
        return self.streams.get(vbucket)

    def _handle_op(self, op, retries=5):
        """ sends op to mcd. Then it recvs response

            if the received response is for another op then it will
            attempt to get the next response and retry 5 times."""

        self.ops[op.opaque] = op
        self.send_op(op)

        response = None
        while retries > 0:
            response = self.recv_op(op)
            if response:
                break
            retries -= 1
            time.sleep(1)

        return response

    def send_op(self, op):
        """ sends op details to mcd client for lowlevel packet assembly """

        self.vbucketId = op.vbucket
        self._sendCmd(op.opcode,
                      op.key,
                      op.value,
                      op.opaque,
                      op.extras)

    def recv_op(self, op):

        while True:
            try:

                opcode, status, opaque, cas, keylen, extlen, dtype, body = \
                    self._recvMsg()

                if self.__opcode_dump:
                    print 'Opcode Dump:', str(hex(opcode)), self.opcode_lookup(opcode)

                if opaque == op.opaque:
                    response = op.formated_response(opcode, keylen,
                                                    extlen, dtype, status,
                                                    cas, body, opaque)
                    return response

                # check if response is for different request
                cached_op = self.ops.get(opaque)
                if cached_op:
                    response = cached_op.formated_response(opcode, keylen,
                                                           extlen, dtype, status,
                                                           cas, body, opaque)
                    # save for later
                    cached_op.queue.put(response)

                elif opcode == CMD_FLOW_CONTROL:
                    # TODO: handle
                    continue

                elif opcode == CMD_STREAM_REQ:
                    # stream_req ops received during add_stream request
                    self.ack_stream_req(opaque)

                elif opcode == CMD_DCP_NOOP:
                    self.ack_dcp_noop_req(opaque)

            except Exception as ex:
                print "recv_op Exception:", ex
                if 'died' in str(ex):
                    return {'opcode': op.opcode,
                            'status': 0xff}
                else:
                    return None

    def ack_stream_req(self, opaque):
        body = struct.pack("<QQ", 123456, 0)
        header = struct.pack(REQ_PKT_FMT,
                             RES_MAGIC_BYTE,
                             CMD_STREAM_REQ,
                             0, 0, 0, 0,
                             len(body), opaque, 0)
        self.s.sendall(header + body)

    def ack_dcp_noop_req(self, opaque):
        # Added function to respond to NOOP's
        header = struct.pack(RES_PKT_FMT,
                             RES_MAGIC_BYTE,
                             CMD_DCP_NOOP,
                             0, 0, 0, 0, 0, opaque, 0)
        self.s.sendall(header)

    def opcode_dump_control(self, control):
        self.__opcode_dump = control
        
    def opcode_lookup(self, opcode):
        from memcacheConstants import DCP_Opcode_Dictionary
        return DCP_Opcode_Dictionary.get(opcode, 'Unknown Opcode')


class DcpStream(object):
    """ DcpStream class manages a stream generator that yields mutations """

    def __init__(self, generator, vbucket):

        self.__generator = generator
        self.vbucket = vbucket
        response = self.__generator.next()
        assert response is not None

        self.failover_log = response.get('failover_log')
        self.err_msg = response.get('err_msg')
        self.status = response.get('status')
        self.rollback = response.get('rollback')
        self.rollback_seqno = response.get('seqno')
        self.opcode = response.get('opcode')
        self.last_by_seqno = 0
        self.mutation_count = 0
        self._ended = False

    def next_response(self):

        if self._ended: return None

        response = self.__generator.next()

        if response:

            if response['opcode'] in (CMD_MUTATION, CMD_DELETION):
                assert int(response['vbucket']) == self.vbucket

                assert 'by_seqno' in response, \
                    "ERROR: vbucket(%s) received mutation without seqno: %s" \
                    % (response['vbucket'], response)
                assert response['by_seqno'] > self.last_by_seqno, \
                    "ERROR: Out of order response on vbucket %s: %s" \
                    % (response['vbucket'], response)
                self.last_by_seqno = response['by_seqno']
                self.mutation_count += 1

            if response['opcode'] == CMD_STREAM_END:
                self._ended = True

        return response

    def has_response(self):
        return not self._ended

    def run(self, to_seqno=MAX_SEQNO, retries=20):

        responses = []

        try:
            while self.has_response():

                r = self.next_response()

                if r is None:
                    retries -= 1
                    assert retries > 0, \
                        "ERROR: vbucket (%s) stream stopped receiving mutations " \
                        % self.vbucket
                    continue
                # add metadata for timing analysis
                r['arrival_time'] = time.time()
                responses.append(r)

                if 'status' in r and r['status'] == 0xff:
                    break

                if self.last_by_seqno >= to_seqno:
                    break

        except StopIteration:
            self._ended = True
            del self.__generator

        return responses


class Operation(object):
    """ Operation Class generically represents any dcp operation providing
        default values for attributes common to each operation """

    def __init__(self, opcode, key='', value='',
                 extras='', vbucket=0, opaque=None):
        self.opcode = opcode
        self.key = key
        self.value = value
        self.extras = extras
        self.vbucket = vbucket
        self.opaque = opaque or random.Random().randint(0, 2 ** 32)
        self.queue = Queue.Queue()

    def formated_response(self, opcode, keylen, extlen, dtype, status, cas, body, opaque):
        return {'opcode': opcode,
                'status': status,
                'body': body}


class Open(Operation):
    """ Open connection base class """

    def __init__(self, name, flag, json):
        opcode = CMD_OPEN
        key = name
        extras = struct.pack(">iI", 0, flag)
        Operation.__init__(self, opcode, key, value=json, extras=extras)


class OpenConsumer(Open):
    """ Open consumer spec """

    def __init__(self, name):
        Open.__init__(self, name, FLAG_OPEN_CONSUMER, json='')


class OpenProducer(Open):
    """ Open producer spec """

    def __init__(self, name, xattr, delete_times, collection, json):
        flags = FLAG_OPEN_PRODUCER
        if xattr:
            flags |= FLAG_OPEN_INCLUDE_XATTRS
        if collection:
            flags |= FLAG_OPEN_COLLECTIONS
        if delete_times:
            flags |= FLAG_OPEN_INCLUDE_DELETE_TIMES
        Open.__init__(self, name, flags, json)


class OpenNotifier(Open):
    """ Open notifier spec """

    def __init__(self, name):
        Open.__init__(self, name, FLAG_OPEN_NOTIFIER, json='')


class CloseStream(Operation):
    """ CloseStream spec """

    def __init__(self, vbucket, takeover=0):
        opcode = CMD_CLOSE_STREAM
        Operation.__init__(self, opcode,
                           vbucket=vbucket)

    def formated_response(self, opcode, keylen, extlen, dtype, status, cas, body, opaque):
        response = {'opcode': opcode,
                    'status': status,
                    'value': body}
        return response


class AddStream(Operation):
    """ AddStream spec """

    def __init__(self, vbucket, takeover=0):
        opcode = CMD_ADD_STREAM
        extras = struct.pack(">I", takeover)
        Operation.__init__(self, opcode,
                           extras=extras,
                           vbucket=vbucket)

    def formated_response(self, opcode, keylen, extlen, status, cas, body, opaque):
        response = {'opcode': opcode,
                    'status': status,
                    'extlen': extlen,
                    'value': body}
        return response


class StreamRequest(Operation):
    """ StreamRequest spec """

    def __init__(self, vbucket, takeover, start_seqno, end_seqno,
                 vb_uuid, snap_start=None, snap_end=None,
                 collections=False, delete_times=False):

        if snap_start is None:
            snap_start = start_seqno
        if snap_end is None:
            snap_end = start_seqno

        opcode = CMD_STREAM_REQ
        extras = struct.pack(">IIQQQQQ", takeover, 0,
                             start_seqno,
                             end_seqno,
                             vb_uuid,
                             snap_start,
                             snap_end)

        Operation.__init__(self, opcode,
                           extras=extras,
                           vbucket=vbucket)
        self.start_seqno = start_seqno
        self.end_seqno = end_seqno
        self.vb_uuid = vb_uuid
        self.takeover = takeover
        self.snap_start = snap_start
        self.snap_end = snap_end
        self.collection_filtering = collections
        self.delete_times = delete_times

    def parse_extended_meta_data(self, extended_meta_data):
        # extended metadata is described here
        # https://github.com/couchbaselabs/dcp-documentation/blob/master/documentation/commands/extended_meta/ext_meta_ver1.md
        # this parsing assumes a fixed format with respect to adjusted time and conflict resolution mode

        adjusted_time = 0
        conflict_resolution_mode = 0

        version, op, op_length = struct.unpack(">BBH", extended_meta_data[0:4])

        if op == META_ADJUSTED_TIME:
            adjusted_time = struct.unpack(">Q", extended_meta_data[4:12])[0]

        elif op == META_CONFLICT_RESOLUTION_MODE:
            conflict_resolution_mode = struct.unpack(">B", extended_meta_data[4:5])[0]
        pos = 1 + 1 + 2 + op_length  # version field, op id + op value

        if len(extended_meta_data) > pos:

            # more to parse
            op, op_length = struct.unpack(">BH", extended_meta_data[pos:pos + 3])
            pos = pos + 3
            if op == META_ADJUSTED_TIME:
                adjusted_time = struct.unpack(">Q", extended_meta_data[pos:pos + 4])[0]

            elif op == META_CONFLICT_RESOLUTION_MODE:
                conflict_resolution_mode = struct.unpack(">B", extended_meta_data[pos:pos + 1])[0]

        return adjusted_time, conflict_resolution_mode

    def parse_extended_attributes(self, value, tot_len):
        xattrs = []
        pos = 0
        while pos < tot_len:
            xattr_len = struct.unpack('>I', value[pos:pos + 4])[0]
            pos = pos + 4
            xattr_key = ""
            xattr_value = ""
            # Extract the Key
            while value[pos] != '\000':
                xattr_key = xattr_key + value[pos]
                pos += 1
            pos += 1
            # Extract the Value
            while value[pos] != '\000':
                xattr_value = xattr_value + value[pos]
                pos += 1

            pos += 1
            xattrs.append((xattr_key, xattr_value))
        return xattrs

    def formated_response(self, opcode, keylen, extlen, dtype, status, cas, body, opaque):
        adjusted_time = None
        conflict_resolution_mode = 0
        xattrs = None
        clen = 0

        if opcode == CMD_STREAM_REQ:

            response = {'opcode': opcode,
                        'status': status,
                        'failover_log': [],
                        'err_msg': None}

            if status == 0:
                assert (len(body) % 16) == 0
                response['failover_log'] = []

                pos = 0
                bodylen = len(body)
                while bodylen > pos:
                    vb_uuid, seqno = struct.unpack(">QQ", body[pos:pos + 16])
                    response['failover_log'].append((vb_uuid, seqno))
                    pos += 16
            elif status == 35:

                seqno = struct.unpack(">II", body)
                response['seqno'] = seqno[0]
                response['rollback'] = seqno[1]

            else:
                response['err_msg'] = body

        elif opcode == CMD_STREAM_END:
            flags = struct.unpack(">I", body[0:4])[0]
            response = {'opcode': opcode,
                        'vbucket': status,
                        'flags': flags}

        elif opcode == CMD_MUTATION:
            if self.collection_filtering:
                header_len = 32
                by_seqno, rev_seqno, flags, exp, lock_time, ext_meta_len, nru, clen = \
                    struct.unpack(">QQIIIHBB", body[0:header_len])
            else:
                header_len = 31
                by_seqno, rev_seqno, flags, exp, lock_time, ext_meta_len, nru = \
                    struct.unpack(">QQIIIHB", body[0:header_len])
            key = body[header_len:header_len + keylen]
            value = body[header_len + keylen: len(body) - ext_meta_len]
            if ext_meta_len > 0:
                adjusted_time, conflict_resolution_mode = self.parse_extended_meta_data(body[len(body) - ext_meta_len:])
            if (dtype & DATATYPE_XATTR):
                total_xattr_len = struct.unpack('>I', value[0:4])[0]
                xattrs = self.parse_extended_attributes(value[4:], total_xattr_len)
                value = value[total_xattr_len + 4:]

            response = {'opcode': opcode,
                        'vbucket': status,
                        'by_seqno': by_seqno,
                        'rev_seqno': rev_seqno,
                        'flags': flags,
                        'expiration': exp,
                        'lock_time': lock_time,
                        'nmeta': ext_meta_len,
                        'nru': nru,
                        'key': key,
                        'value': value,
                        'adjusted_time': adjusted_time,
                        'conflict_resolution_mode': conflict_resolution_mode,
                        'xattrs': xattrs,
                        'collection_len': clen}

        elif opcode == CMD_DELETION:
            delete_time = 0
            ext_meta_len = 0
            if self.collection_filtering or self.delete_times:
                header_len = 21
                by_seqno, rev_seqno, delete_time, clen = \
                    struct.unpack(">QQIB", body[0:21])
            else:
                header_len = 18
                by_seqno, rev_seqno, ext_meta_len = \
                    struct.unpack(">QQH", body[0:18])

            print delete_time
            key = body[header_len:header_len + keylen]

            if ext_meta_len > 0:
                adjusted_time, conflict_resolution_mode = \
                    self.parse_extended_meta_data(body[len(body) - ext_meta_len:])

            response = {'opcode': opcode,
                        'vbucket': status,
                        'by_seqno': by_seqno,
                        'rev_seqno': rev_seqno,
                        'key': key,
                        'adjusted_time': adjusted_time,
                        'conflict_resolution_mode': conflict_resolution_mode,
                        'delete_time': delete_time,
                        'collection_len': clen}

        elif opcode == CMD_SNAPSHOT_MARKER:

            assert len(body) == 20
            snap_start, snap_end, flag = \
                struct.unpack(">QQI", body)
            assert flag in (1, 2, 5, 6), "Invalid snapshot flag: %s" % flag
            assert snap_start <= snap_end, "Snapshot start: %s > end: %s" % \
                                           (snap_start, snap_end)
            flag = {1: 'memory',
                    2: 'disk',
                    5: 'memory-checkpoint',
                    6: 'disk-checkpoint'}[flag]

            response = {'opcode': opcode,
                        'vbucket': status,
                        'snap_start_seqno': snap_start,
                        'snap_end_seqno': snap_end,
                        'flag': flag}

        elif opcode == 0x5f:
            seqno, event = \
                struct.unpack(">QI", body[0:12])
            key = body[12:12 + keylen]
            response = {'opcode': opcode,
                        'seqno': seqno,
                        'event': event,
                        'key': key}
        else:
            response = {'err_msg': "(Stream Request) Unknown response",
                        'opcode': opcode,
                        'status': -1}

        return response


class GetFailoverLog(Operation):
    """ GetFailoverLog spec """

    def __init__(self, vbucket):
        opcode = CMD_GET_FAILOVER_LOG
        Operation.__init__(self, opcode,
                           vbucket=vbucket)

    def formated_response(self, opcode, keylen, extlen, dtype, status, cas, body, opaque):

        failover_log = []

        if status == 0:
            assert len(body) % 16 == 0
            pos = 0
            bodylen = len(body)
            while bodylen > pos:
                vb_uuid, seqno = struct.unpack(">QQ", body[pos:pos + 16])
                failover_log.append((vb_uuid, seqno))
                pos += 16

        response = {'opcode': opcode,
                    'status': status,
                    'value': failover_log}
        return response


class FlowControl(Operation):
    """ FlowControl spec """

    def __init__(self, buffer_size):
        opcode = CMD_CONTROL
        Operation.__init__(self, opcode,
                           key="connection_buffer_size",
                           value=str(buffer_size))

    def formated_response(self, opcode, keylen, extlen, dtype, status, cas, body, opaque):
        response = {'opcode': opcode,
                    'status': status,
                    'body': body}
        return response


class GeneralControl(Operation):

    def __init__(self, key, value):
        opcode = CMD_CONTROL
        Operation.__init__(self, opcode,
                           key,
                           value)

    def formated_response(self, opcode, keylen, extlen, dtype, status, cas, body, opaque):
        response = {'opcode': opcode,
                    'status': status,
                    'body': body}
        return response


class Ack(Operation):
    """ Ack spec """

    def __init__(self, nbytes):
        opcode = CMD_DCP_ACK
        self.nbytes = nbytes
        extras = struct.pack(">L", self.nbytes)
        Operation.__init__(self, opcode,
                           extras=extras)

    def formated_response(self, opcode, keylen, extlen, dtype, status, cas, body, opaque):
        response = {'opcode': opcode,
                    'status': status,
                    'error': body}
        return response


class Quit(Operation):

    def __init__(self):
        opcode = CMD_QUIT
        Operation.__init__(self, opcode)

    def formated_response(self, opcode, keylen, extlen, dtype, status, cas, body, opaque):
        response = {'opcode': opcode,
                    'status': status}
        return response
