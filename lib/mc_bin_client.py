#!/usr/bin/env python
"""
Binary memcached test client.

Copyright (c) 2007  Dustin Sallings <dustin@spy.net>
"""

import array
import hmac
import socket
import select
import random
import struct
import exceptions
import zlib
from rest_client import RestClient

from memcacheConstants import REQ_MAGIC_BYTE, RES_MAGIC_BYTE
from memcacheConstants import REQ_PKT_FMT, RES_PKT_FMT, MIN_RECV_PACKET
from memcacheConstants import SET_PKT_FMT, INCRDECR_RES_FMT
import memcacheConstants

def decodeCollectionID(key):
    # A leb128 varint encodes the CID
    data = array.array('B', key)
    cid = data[0] & 0x7f
    end = 1
    if (data[0] & 0x80) == 0x80:
        shift =7
        for end in range(1, len(data)):
            cid |= ((data[end] & 0x7f) << shift)
            if (data[end] & 0x80) == 0:
                break
            shift = shift + 7

        end = end + 1
        if end == len(data):
            #  We should of stopped for a stop byte, not the end of the buffer
            raise exceptions.ValueError("encoded key did not contain a stop byte")
    return cid, key[end:]

class MemcachedError(exceptions.Exception):
    """Error raised when a command fails."""

    def __init__(self, status, msg):
        error_msg = error_to_str(status)
        supermsg = 'Memcached error #' + `status` + ' ' + `error_msg`
        if msg: supermsg += ":  " + msg
        exceptions.Exception.__init__(self, supermsg)

        self.status = status
        self.msg = msg

    def __repr__(self):
        return "<MemcachedError #%d ``%s''>" % (self.status, self.msg)

class MemcachedClient(object):
    """Simple memcached client."""

    vbucketId = 0

    def __init__(self, host='127.0.0.1', port=11211, timeout=30, admin_user="cbadminbucket",
                  admin_pass="password", rest_port=8091, do_auth=True):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._createConn()
        self.r = random.Random()
        self.vbucket_count = 1024
        if do_auth:
            self.sasl_auth_plain(admin_user, admin_pass)

            # auth on any existing buckets
            rest_client = RestClient(host, port=rest_port)
            for bucket in rest_client.get_all_buckets():
               try:
                  self.bucket_select(bucket)
               except Exception as ex:
                  # can be ignored...possibly warming up
                  pass

    def _createConn(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return self.s.connect_ex((self.host, self.port))

    def reconnect(self):
        self.s.close()
        return self._createConn()

    def close(self):
        self.s.close()

    def __del__(self):
        self.close()

    def _sendCmd(self, cmd, key, val, opaque, extraHeader='', cas=0):
        self._sendMsg(cmd, key, val, opaque, extraHeader=extraHeader, cas=cas,
                      vbucketId=self.vbucketId)

    def _sendMsg(self, cmd, key, val, opaque, extraHeader='', cas=0,
                 dtype=0, vbucketId=0,
                 fmt=REQ_PKT_FMT, magic=REQ_MAGIC_BYTE):
        msg = struct.pack(fmt, magic,
            cmd, len(key), len(extraHeader), dtype, vbucketId,
                len(key) + len(extraHeader) + len(val), opaque, cas)
        _, w, _ = select.select([], [self.s], [], self.timeout)
        if w:
            self.s.sendall(msg + extraHeader + key + val)
        else:
            raise exceptions.EOFError("Timeout waiting for socket send. from {0}".format(self.host))

    def _recvMsg(self):
        response = ""
        while len(response) < MIN_RECV_PACKET:
            r, _, _ = select.select([self.s], [], [], self.timeout)
            if r:
                data = self.s.recv(MIN_RECV_PACKET - len(response))
                if data == '':
                    raise exceptions.EOFError("Got empty data (remote died?). from {0}".format(self.host))
                response += data
            else:
                raise exceptions.EOFError("Timeout waiting for socket recv. from {0}".format(self.host))
        assert len(response) == MIN_RECV_PACKET
        magic, cmd, keylen, extralen, dtype, errcode, remaining, opaque, cas = \
            struct.unpack(RES_PKT_FMT, response)

        rv = ""
        while remaining > 0:
            r, _, _ = select.select([self.s], [], [], self.timeout)
            if r:
                data = self.s.recv(remaining)
                if data == '':
                    raise exceptions.EOFError("Got empty data (remote died?). from {0}".format(self.host))
                rv += data
                remaining -= len(data)
            else:
                raise exceptions.EOFError("Timeout waiting for socket recv. from {0}".format(self.host))

        assert (magic in (RES_MAGIC_BYTE, REQ_MAGIC_BYTE)), "Got magic: %d" % magic
        return cmd, errcode, opaque, cas, keylen, extralen, dtype, rv

    def _handleKeyedResponse(self, myopaque):
        cmd, errcode, opaque, cas, keylen, extralen, dtype, rv = self._recvMsg()
        assert myopaque is None or opaque == myopaque, \
            "expected opaque %x, got %x" % (myopaque, opaque)
        if errcode:
            rv += " for vbucket :{0} to mc {1}:{2}".format(self.vbucketId, self.host, self.port)
            raise MemcachedError(errcode, rv)
        return cmd, opaque, cas, keylen, extralen, rv

    def _handleSingleResponse(self, myopaque):
        cmd, opaque, cas, keylen, extralen, data = self._handleKeyedResponse(myopaque)
        return opaque, cas, data

    def _doCmd(self, cmd, key, val, extraHeader='', cas=0):
        """Send a command and await its response."""
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(cmd, key, val, opaque, extraHeader, cas)
        return self._handleSingleResponse(opaque)



    def _doSdCmd(self, cmd, key, path, val=None, expiry=0, opaque=0, cas=0, create=False, xattr=None):
        createFlag = 0
        if opaque == 0:
            opaque = self.r.randint(0, 2**32)
        if create:
            flag = memcacheConstants.SUBDOC_FLAGS_MKDIR_P
        if xattr:
            flag = memcacheConstants.SUBDOC_FLAG_XATTR_PATH
        extraHeader = struct.pack(memcacheConstants.REQ_PKT_SD_EXTRAS, len(path), flag)
        body = path
        if val != None:
            body += str(val)
        self._sendCmd(cmd, key, body, opaque, extraHeader, cas)
        return self._handleSingleResponse(opaque)

    def _doMultiSdCmd(self, cmd, key, cmdDict, opaque=0):
        if opaque == 0:
            opaque = self.r.randint(0, 2**32)
        body = ''
        extraHeader = ''
        mcmd = None
        for k, v  in cmdDict.iteritems():
            if k == "store":
                mcmd = memcacheConstants.CMD_SUBDOC_DICT_ADD
            elif k == "counter":
                mcmd = memcacheConstants.CMD_SUBDOC_COUNTER
            elif k == "add_unique":
                mcmd = memcacheConstants.CMD_SUBDOC_ARRAY_ADD_UNIQUE
            elif k == "push_first":
                mcmd = memcacheConstants.CMD_SUBDOC_ARRAY_PUSH_FIRST
            elif k == "push_last":
                mcmd = memcacheConstants.CMD_SUBDOC_ARRAY_PUSH_LAST
            elif k == "array_insert":
                mcmd = memcacheConstants.CMD_SUBDOC_ARRAY_INSERT
            elif k == "insert":
                mcmd = memcacheConstants.CMD_SUBDOC_DICT_ADD
            if v['create_parents'] == True:
                flags = 1
            else:
                flags = 0
            path = v['path']
            valuelen = 0
            if isinstance(v["value"], str):
                value = '"' + v['value'] + '"'
                valuelen = len(value)
            elif isinstance(v["value"], int):
                value = str(v['value'])
                valuelen = len(str(value))
            op_spec = struct.pack(memcacheConstants.REQ_PKT_SD_MULTI_MUTATE, mcmd, flags, len(path), valuelen)
            op_spec += path + value
            body += op_spec
        self._sendMsg(cmd, key, body, opaque, extraHeader, cas=0)
        return self._handleSingleResponse(opaque)

    def _mutate(self, cmd, key, exp, flags, cas, val):
        return self._doCmd(cmd, key, val, struct.pack(SET_PKT_FMT, flags, exp),
            cas)

    def _cat(self, cmd, key, cas, val):
        return self._doCmd(cmd, key, val, '', cas)

    def append(self, key, value, cas=0, vbucket= -1):
        self._set_vbucket(key, vbucket)
        return self._cat(memcacheConstants.CMD_APPEND, key, cas, value)

    def prepend(self, key, value, cas=0, vbucket= -1):
        self._set_vbucket(key, vbucket)
        return self._cat(memcacheConstants.CMD_PREPEND, key, cas, value)

    def __incrdecr(self, cmd, key, amt, init, exp):
        something, cas, val = self._doCmd(cmd, key, '',
            struct.pack(memcacheConstants.INCRDECR_PKT_FMT, amt, init, exp))
        return struct.unpack(INCRDECR_RES_FMT, val)[0], cas

    def incr(self, key, amt=1, init=0, exp=0, vbucket= -1):
        """Increment or create the named counter."""
        self._set_vbucket(key, vbucket)
        return self.__incrdecr(memcacheConstants.CMD_INCR, key, amt, init, exp)

    def decr(self, key, amt=1, init=0, exp=0, vbucket= -1):
        """Decrement or create the named counter."""
        self._set_vbucket(key, vbucket)
        return self.__incrdecr(memcacheConstants.CMD_DECR, key, amt, init, exp)

    def set(self, key, exp, flags, val, vbucket= -1):
        """Set a value in the memcached server."""
        self._set_vbucket(key, vbucket)
        return self._mutate(memcacheConstants.CMD_SET, key, exp, flags, 0, val)

    def send_set(self, key, exp, flags, val, vbucket= -1):
        """Set a value in the memcached server without handling the response"""
        self._set_vbucket(key, vbucket)
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(memcacheConstants.CMD_SET, key, val, opaque, struct.pack(SET_PKT_FMT, flags, exp), 0)

    def send_snapshot_marker(self, start, end, vbucket= -1):
        """Set a value in the memcached server without handling the response"""
        #self._set_vbucket(key, vbucket)
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(memcacheConstants.CMD_SNAPSHOT_MARKER, '', '', opaque, struct.pack("QQI", start, end, 1), 0)
        return self._handleSingleResponse(opaque)


    def add(self, key, exp, flags, val, vbucket= -1):
        """Add a value in the memcached server iff it doesn't already exist."""
        self._set_vbucket(key, vbucket)
        return self._mutate(memcacheConstants.CMD_ADD, key, exp, flags, 0, val)

    def replace(self, key, exp, flags, val, vbucket= -1):
        """Replace a value in the memcached server iff it already exists."""
        self._set_vbucket(key, vbucket)
        return self._mutate(memcacheConstants.CMD_REPLACE, key, exp, flags, 0,
            val)
    def observe(self, key, vbucket= -1):
        """Observe a key for persistence and replication."""
        self._set_vbucket(key, vbucket)
        value = struct.pack('>HH', self.vbucketId, len(key)) + key
        opaque, cas, data = self._doCmd(memcacheConstants.CMD_OBSERVE, '', value)
        rep_time = (cas & 0xFFFFFFFF)
        persist_time = (cas >> 32) & 0xFFFFFFFF
        persisted = struct.unpack('>B', data[4 + len(key)])[0]
        return opaque, rep_time, persist_time, persisted, cas

    def __parseGet(self, data, klen=0):
        flags = struct.unpack(memcacheConstants.GET_RES_FMT, data[-1][:4])[0]
        return flags, data[1], data[-1][4 + klen:]

    def get(self, key, vbucket= -1):
        """Get the value for a given key within the memcached server."""
        self._set_vbucket(key, vbucket)
        parts = self._doCmd(memcacheConstants.CMD_GET, key, '')

        return self.__parseGet(parts)

    def send_get(self, key, vbucket= -1):
        """ sends a get message without parsing the response """
        self._set_vbucket(key, vbucket)
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(memcacheConstants.CMD_GET, key, '', opaque)

    def getl(self, key, exp=15, vbucket= -1):
        """Get the value for a given key within the memcached server."""
        self._set_vbucket(key, vbucket)
        parts = self._doCmd(memcacheConstants.CMD_GET_LOCKED, key, '',
            struct.pack(memcacheConstants.GETL_PKT_FMT, exp))
        return self.__parseGet(parts)

    def getr(self, key, vbucket= -1):
        """Get the value for a given key within the memcached server from a replica vbucket."""
        self._set_vbucket(key, vbucket)
        parts = self._doCmd(memcacheConstants.CMD_GET_REPLICA, key, '')
        return self.__parseGet(parts, len(key))


    def getMeta(self, key):
        """Get the metadata for a given key within the memcached server."""
        self._set_vbucket(key)
        opaque, cas, data = self._doCmd(memcacheConstants.CMD_GET_META, key, '')
        deleted = struct.unpack('>I', data[0:4])[0]
        flags = struct.unpack('>I', data[4:8])[0]
        exp = struct.unpack('>I', data[8:12])[0]
        seqno = struct.unpack('>Q', data[12:20])[0]
        return (deleted, flags, exp, seqno, cas)

    def cas(self, key, exp, flags, oldVal, val, vbucket= -1):
        """CAS in a new value for the given key and comparison value."""
        self._set_vbucket(key, vbucket)
        self._mutate(memcacheConstants.CMD_SET, key, exp, flags,
            oldVal, val)


    def create_checkpoint(self, vbucket= -1):
        return self._doCmd(memcacheConstants.CMD_CREATE_CHECKPOINT, '', '')

    def compact_db(self, key, vbucket, purge_before_ts, purge_before_seq, drop_deletes):
        self._set_vbucket(key, vbucket)
        self.vbucketId = 0
        # some filler bytes were needed
        return self._doCmd(memcacheConstants.CMD_COMPACT_DB, '', '',
                           struct.pack('>QQBBHL', purge_before_ts, purge_before_seq, drop_deletes, 0, 0, 0))


    def hello(self, features, name=''):
        """Send a hello command for feature checking"""
        #MB-11902
        opaque, cas, data = self._doCmd(memcacheConstants.CMD_HELO, name, struct.pack('>' + ('H' * len(features)), *features))
        return struct.unpack('>' + ('H' * (len(data)/2)), data)

    def touch(self, key, exp, vbucket= -1):
        """Touch a key in the memcached server."""
        self._set_vbucket(key, vbucket)
        return self._doCmd(memcacheConstants.CMD_TOUCH, key, '',
            struct.pack(memcacheConstants.TOUCH_PKT_FMT, exp))

    def gat(self, key, exp, vbucket= -1):
        """Get the value for a given key and touch it within the memcached server."""
        self._set_vbucket(key, vbucket)
        parts = self._doCmd(memcacheConstants.CMD_GAT, key, '',
            struct.pack(memcacheConstants.GAT_PKT_FMT, exp))
        return self.__parseGet(parts)

    def version(self):
        """Get the value for a given key within the memcached server."""
        return self._doCmd(memcacheConstants.CMD_VERSION, '', '')

    def sasl_mechanisms(self):
        """Get the supported SASL methods."""
        return set(self._doCmd(memcacheConstants.CMD_SASL_LIST_MECHS,
                               '', '')[2].split(' '))

    def sasl_auth_start(self, mech, data):
        """Start a sasl auth session."""
        return self._doCmd(memcacheConstants.CMD_SASL_AUTH, mech, data)

    def sasl_auth_plain(self, user, password, foruser=''):
        """Perform plain auth."""
        return self.sasl_auth_start('PLAIN', '\0'.join([foruser, user, password]))

    def sasl_auth_cram_md5(self, user, password):
        """Start a plan auth session."""
        challenge = None
        try:
            self.sasl_auth_start('CRAM-MD5', '')
        except MemcachedError, e:
            if e.status != memcacheConstants.ERR_AUTH_CONTINUE:
                raise
            challenge = e.msg.split(' ')[0]

        dig = hmac.HMAC(password, challenge).hexdigest()
        return self._doCmd(memcacheConstants.CMD_SASL_STEP, 'CRAM-MD5',
                           user + ' ' + dig)

    def stop_persistence(self):
        return self._doCmd(memcacheConstants.CMD_STOP_PERSISTENCE, '', '')

    def start_persistence(self):
        return self._doCmd(memcacheConstants.CMD_START_PERSISTENCE, '', '')

    def set_flush_param(self, key, val):
        print "setting flush param:", key, val
        return self._doCmd(memcacheConstants.CMD_SET_FLUSH_PARAM, key, val)

    def set_param(self, key, val, type):
        print "setting param:", key, val
        type = struct.pack(memcacheConstants.GET_RES_FMT, type)
        return self._doCmd(memcacheConstants.CMD_SET_FLUSH_PARAM, key, val, type)

    def start_onlineupdate(self):
        return self._doCmd(memcacheConstants.CMD_START_ONLINEUPDATE, '', '')

    def complete_onlineupdate(self):
        return self._doCmd(memcacheConstants.CMD_COMPLETE_ONLINEUPDATE, '', '')

    def revert_onlineupdate(self):
        return self._doCmd(memcacheConstants.CMD_REVERT_ONLINEUPDATE, '', '')

    def set_tap_param(self, key, val):
        print "setting tap param:", key, val
        return self._doCmd(memcacheConstants.CMD_SET_TAP_PARAM, key, val)

    def set_vbucket_state(self, vbucket, stateName):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        state = struct.pack(memcacheConstants.VB_SET_PKT_FMT,
                            memcacheConstants.VB_STATE_NAMES[stateName])
        return self._doCmd(memcacheConstants.CMD_SET_VBUCKET_STATE, '', '', state)

    def get_vbucket_state(self, vbucket):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        return self._doCmd(memcacheConstants.CMD_GET_VBUCKET_STATE,
                           str(vbucket), '')

    def get_vbucket_all_vbucket_seqnos(self):
        return self._doCmd(memcacheConstants.CMD_GET_ALL_VB_SEQNOS,  '', '')

    def delete_vbucket(self, vbucket):
        assert isinstance(vbucket, int)
        self.vbucketId = vbucket
        return self._doCmd(memcacheConstants.CMD_DELETE_VBUCKET, '', '')

    def evict_key(self, key, vbucket= -1):
        self._set_vbucket(key, vbucket)
        return self._doCmd(memcacheConstants.CMD_EVICT_KEY, key, '')

    def getMulti(self, keys, vbucket= -1):
        """Get values for any available keys in the given iterable.

        Returns a dict of matched keys to their values."""
        opaqued = dict(enumerate(keys))
        terminal = len(opaqued) + 10
        # Send all of the keys in quiet
        vbs = set()
        for k, v in opaqued.iteritems():
            self._set_vbucket(v, vbucket)
            vbs.add(self.vbucketId)
            self._sendCmd(memcacheConstants.CMD_GETQ, v, '', k)

        for vb in vbs:
            self.vbucketId = vb
            self._sendCmd(memcacheConstants.CMD_NOOP, '', '', terminal)

        # Handle the response
        rv = {}
        for vb in vbs:
            self.vbucketId = vb
            done = False
            while not done:
                opaque, cas, data = self._handleSingleResponse(None)
                if opaque != terminal:
                    rv[opaqued[opaque]] = self.__parseGet((opaque, cas, data))
                else:
                    done = True

        return rv

    def setMulti(self, exp, flags, items, vbucket= -1):
        """Multi-set (using setq).

        Give me (key, value) pairs."""

        # If this is a dict, convert it to a pair generator
        if hasattr(items, 'iteritems'):
            items = items.iteritems()

        opaqued = dict(enumerate(items))
        terminal = len(opaqued) + 10
        extra = struct.pack(SET_PKT_FMT, flags, exp)

        # Send all of the keys in quiet
        vbs = set()
        for opaque, kv in opaqued.iteritems():
            self._set_vbucket(kv[0], vbucket)
            vbs.add(self.vbucketId)
            self._sendCmd(memcacheConstants.CMD_SETQ, kv[0], kv[1], opaque, extra)

        for vb in vbs:
            self.vbucketId = vb
            self._sendCmd(memcacheConstants.CMD_NOOP, '', '', terminal)

        # Handle the response
        failed = []
        for vb in vbs:
            self.vbucketId = vb
            done = False
            while not done:
                try:
                    opaque, cas, data = self._handleSingleResponse(None)
                    done = opaque == terminal
                except MemcachedError, e:
                    failed.append(e)

        return failed

    def stats(self, sub=''):
        """Get stats."""
        opaque = self.r.randint(0, 2 ** 32)
        self._sendCmd(memcacheConstants.CMD_STAT, sub, '', opaque)
        done = False
        rv = {}
        while not done:
            cmd, opaque, cas, klen, extralen, data = self._handleKeyedResponse(None)
            if klen:
                rv[data[0:klen]] = data[klen:]
            else:
                done = True
        return rv

    def noop(self):
        """Send a noop command."""
        return self._doCmd(memcacheConstants.CMD_NOOP, '', '')

    def delete(self, key, cas=0, vbucket= -1):
        """Delete the value for a given key within the memcached server."""
        self._set_vbucket(key, vbucket)
        return self._doCmd(memcacheConstants.CMD_DELETE, key, '', '', cas)

    def flush(self, timebomb=0):
        """Flush all storage in a memcached instance."""
        return self._doCmd(memcacheConstants.CMD_FLUSH, '', '',
            struct.pack(memcacheConstants.FLUSH_PKT_FMT, timebomb))

    def bucket_select(self, name):
        return self._doCmd(memcacheConstants.CMD_SELECT_BUCKET, name, '')

    def sync_persistence(self, keyspecs):
        payload = self._build_sync_payload(0x8, keyspecs)

        print "sending sync for persistence command for the following keyspecs:", keyspecs
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return (opaque, cas, self._parse_sync_response(data))

    def sync_mutation(self, keyspecs):
        payload = self._build_sync_payload(0x4, keyspecs)

        print "sending sync for mutation command for the following keyspecs:", keyspecs
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return (opaque, cas, self._parse_sync_response(data))

    def sync_replication(self, keyspecs, numReplicas=1):
        payload = self._build_sync_payload((numReplicas & 0x0f) << 4, keyspecs)

        print "sending sync for replication command for the following keyspecs:", keyspecs
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return (opaque, cas, self._parse_sync_response(data))

    def sync_replication_or_persistence(self, keyspecs, numReplicas=1):
        payload = self._build_sync_payload(((numReplicas & 0x0f) << 4) | 0x8, keyspecs)

        print "sending sync for replication or persistence command for the " \
            "following keyspecs:", keyspecs
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return (opaque, cas, self._parse_sync_response(data))

    def sync_replication_and_persistence(self, keyspecs, numReplicas=1):
        payload = self._build_sync_payload(((numReplicas & 0x0f) << 4) | 0xA, keyspecs)

        print "sending sync for replication and persistence command for the " \
            "following keyspecs:", keyspecs
        (opaque, cas, data) = self._doCmd(memcacheConstants.CMD_SYNC, "", payload)
        return (opaque, cas, self._parse_sync_response(data))

    def set_time_drift_counter_state(self, vbucket, drift, state):
        """Get the value for a given key within the memcached server."""
        self.vbucketId = vbucket
        extras = struct.pack(memcacheConstants.SET_DRIFT_COUNTER_STATE_REQ_FMT, drift, state)
        return self._doCmd(memcacheConstants.CMD_SET_DRIFT_COUNTER_STATE, '', '', extras)


    def _build_sync_payload(self, flags, keyspecs):
        payload = struct.pack(">I", flags)
        payload += struct.pack(">H", len(keyspecs))

        for spec in keyspecs:
            if not isinstance(spec, dict):
                raise TypeError("each keyspec must be a dict")
            if not spec.has_key('vbucket'):
                raise TypeError("missing vbucket property in keyspec")
            if not spec.has_key('key'):
                raise TypeError("missing key property in keyspec")

            payload += struct.pack(">Q", spec.get('cas', 0))
            payload += struct.pack(">H", spec['vbucket'])
            payload += struct.pack(">H", len(spec['key']))
            payload += spec['key']

        return payload

    def _parse_sync_response(self, data):
        keyspecs = []
        nkeys = struct.unpack(">H", data[0 : struct.calcsize("H")])[0]
        offset = struct.calcsize("H")

        for i in xrange(nkeys):
            spec = {}
            width = struct.calcsize("QHHB")
            (spec['cas'], spec['vbucket'], keylen, eventid) = \
                struct.unpack(">QHHB", data[offset : offset + width])
            offset += width
            spec['key'] = data[offset : offset + keylen]
            offset += keylen

            if eventid == memcacheConstants.CMD_SYNC_EVENT_PERSISTED:
                spec['event'] = 'persisted'
            elif eventid == memcacheConstants.CMD_SYNC_EVENT_MODIFED:
                spec['event'] = 'modified'
            elif eventid == memcacheConstants.CMD_SYNC_EVENT_DELETED:
                spec['event'] = 'deleted'
            elif eventid == memcacheConstants.CMD_SYNC_EVENT_REPLICATED:
                spec['event'] = 'replicated'
            elif eventid == memcacheConstants.CMD_SYNC_INVALID_KEY:
                spec['event'] = 'invalid key'
            elif spec['event'] == memcacheConstants.CMD_SYNC_INVALID_CAS:
                spec['event'] = 'invalid cas'
            else:
                spec['event'] = eventid

            keyspecs.append(spec)

        return keyspecs

    def restore_file(self, filename):
        """Initiate restore of a given file."""
        return self._doCmd(memcacheConstants.CMD_RESTORE_FILE, filename, '', '', 0)

    def restore_complete(self):
        """Notify the server that we're done restoring."""
        return self._doCmd(memcacheConstants.CMD_RESTORE_COMPLETE, '', '', '', 0)

    def deregister_tap_client(self, tap_name):
        """Deregister the TAP client with a given name."""
        return self._doCmd(memcacheConstants.CMD_DEREGISTER_TAP_CLIENT, tap_name, '', '', 0)

    def reset_replication_chain(self):
        """Reset the replication chain."""
        return self._doCmd(memcacheConstants.CMD_RESET_REPLICATION_CHAIN, '', '', '', 0)

    def _set_vbucket(self, key, vbucket= -1):
        if vbucket < 0:
            self.vbucketId = (((zlib.crc32(key)) >> 16) & 0x7fff) & (self.vbucket_count - 1)
        else:
            self.vbucketId = vbucket

    def get_config(self):
        """Get the config within the memcached server."""
        return self._doCmd(memcacheConstants.CMD_GET_CLUSTER_CONFIG, '', '')

    def set_config(self, blob_conf):
        """Set the config within the memcached server."""
        return self._doCmd(memcacheConstants.CMD_SET_CLUSTER_CONFIG, blob_conf, '')

    def sd_function(fn):
        def new_func(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception:
                raise
        return new_func

    @sd_function
    def get_sd(self, key, path, cas=0, vbucket= -1):
        self._set_vbucket(key, vbucket)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_GET, key, path, cas=cas)

    @sd_function
    def exists_sd(self, key, path, opaque=0, cas=0, vbucket= -1):
        self._set_vbucket(key, vbucket)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_EXISTS, key, path, opaque=opaque, cas=cas)

    @sd_function
    def dict_add_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1, xattr=None):
        self._set_vbucket(key, vbucket)
        #return self._doSdCmd(memcacheConstants.CMD_SUBDOC_DICT_ADD, key, path, value, expiry, opaque, cas, create, xattr)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_DICT_ADD, key, path, value, expiry, opaque, cas, create)

    @sd_function
    def dict_upsert_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1):
        self._set_vbucket(key, vbucket)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_DICT_UPSERT, key, path, value, expiry, opaque, cas, create)

    @sd_function
    def delete_sd(self, key, path, opaque=0, cas=0, vbucket= -1):
        self._set_vbucket(key, vbucket)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_DELETE, key, path, opaque=opaque, cas=cas)

    @sd_function
    def replace_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1):
        self._set_vbucket(key, vbucket)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_REPLACE, key, path, value, expiry, opaque, cas, create)

    @sd_function
    def array_push_last_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1):
        self._set_vbucket(key, vbucket)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_ARRAY_PUSH_LAST, key, path, value, expiry, opaque, cas, create)

    @sd_function
    def array_push_first_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1):
        self._set_vbucket(key, vbucket)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_ARRAY_PUSH_FIRST, key, path, value, expiry, opaque, cas, create)

    @sd_function
    def array_add_unique_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1):
        self._set_vbucket(key, vbucket)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_ARRAY_ADD_UNIQUE, key, path, value, expiry, opaque, cas, create)

    @sd_function
    def array_add_insert_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1):
        self._set_vbucket(key, vbucket)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_ARRAY_INSERT, key, path, value, expiry, opaque, cas, create)

    @sd_function
    def counter_sd(self, key, path, value, expiry=0, opaque=0, cas=0, create=False, vbucket= -1):
        self._set_vbucket(key, vbucket)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_COUNTER, key, path, value, expiry, opaque, cas, create)


    '''
    usage:
    cmdDict["add_unique"] = {"create_parents" : False, "path": array, "value": 0}
    res  = mc.multi_mutation_sd(key, cmdDict)
    '''
    @sd_function
    def multi_mutation_sd(self, key, cmdDict, expiry=0, opaque=0, cas=0, create=False, vbucket= -1):
        self._set_vbucket(key, vbucket)
        return self._doMultiSdCmd(memcacheConstants.CMD_SUBDOC_MULTI_MUTATION, key, cmdDict, opaque)

    @sd_function
    def multi_lookup_sd(self, key, path, expiry=0, opaque=0, cas=0, create=False, vbucket= -1):
        self._set_vbucket(key, vbucket)
        return self._doSdCmd(memcacheConstants.CMD_SUBDOC_MULTI_LOOKUP, key, path, expiry, opaque, cas, create)

def error_to_str(errno):
    if errno == 0x01:
        return "Not found"
    elif errno == 0x02:
        return "Exists"
    elif errno == 0x03:
        return "Too big"
    elif errno == 0x04:
        return "Invalid"
    elif errno == 0x05:
        return "Not stored"
    elif errno == 0x06:
        return "Bad Delta"
    elif errno == 0x07:
        return "Not my vbucket"
    elif errno == 0x20:
        return "Auth error"
    elif errno == 0x21:
        return "Auth continue"
    elif errno == 0x81:
        return "Unknown Command"
    elif errno == 0x82:
        return "No Memory"
    elif errno == 0x83:
        return "Not Supported"
    elif errno == 0x84:
        return "Internal error"
    elif errno == 0x85:
        return "Busy"
    elif errno == 0x86:
        return "Temporary failure"
    elif errno == memcacheConstants.ERR_SUBDOC_PATH_ENOENT:
        return "Path not exists"
    elif errno == memcacheConstants.ERR_SUBDOC_PATH_MISMATCH:
        return "Path mismatch"
    elif errno == memcacheConstants.ERR_SUBDOC_PATH_EEXISTS:
        return "Path exists already"
    elif errno == memcacheConstants.ERR_SUBDOC_PATH_EINVAL:
        return "Invalid path"
    elif errno == memcacheConstants.ERR_SUBDOC_PATH_E2BIG:
        return "Path too big"
    elif errno == memcacheConstants.ERR_SUBDOC_VALUE_CANTINSERT:
        return "Cant insert"
    elif errno == memcacheConstants.ERR_SUBDOC_DOC_NOTJSON:
        return "Not json"
    elif errno == memcacheConstants.ERR_SUBDOC_NUM_ERANGE:
        return "Num out of range"
    elif errno == memcacheConstants.ERR_SUBDOC_DELTA_ERANGE:
        return "Delta out of range"
    elif errno == memcacheConstants.ERR_SUBDOC_DOC_ETOODEEP:
        return "Doc too deep"
    elif errno == memcacheConstants.ERR_SUBDOC_VALUE_TOODEEP:
        return "Value too deep"
    elif errno == memcacheConstants.ERR_SUBDOC_INVALID_CMD_COMBO:
        return "Invalid combinations of commands"
    elif errno == memcacheConstants.ERR_SUBDOC_MULTI_PATH_FAILURE:
        return "Specified key was successfully found, but one or more path operations failed"
