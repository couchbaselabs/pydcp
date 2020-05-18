#!/usr/bin/env python2

import argparse
import copy
import json
import os
import sys
import uuid

from dcp_data_persist import LogData
from lib.dcp_bin_client import DcpClient
from lib.mc_bin_client import MemcachedError, error_to_str
from lib.memcacheConstants import *


def check_for_features(dcp_client, xattrs=False, collections=False, compression=False):
    features = [HELO_XERROR]
    if xattrs:
        features.append(HELO_XATTR)
    if collections:
        features.append(HELO_COLLECTIONS)
    if compression:
        features.append(HELO_SNAPPY)
    resp = dcp_client.hello(features, "pydcp feature HELO")
    for feature in features:
        assert feature in resp


def handle_stream_create_response(dcpStream, args):
    if dcpStream.status == SUCCESS:
        print 'Stream Opened Successfully on vb', dcpStream.vbucket

        if args.failover_logging and not args.keep_logs:  # keep_logs implies that there is a set of JSON log files
            dcp_log_data.upsert_failover(dcpStream.vbucket, dcpStream.failover_log)
        return None

    elif dcpStream.status == ERR_NOT_MY_VBUCKET:
        print "NOT MY VBUCKET -", dcpStream.vbucket, 'does not live on this node'
        # TODO: Handle that vbucket not entering the stream list
        sys.exit(1)

    elif dcpStream.status == ERR_ROLLBACK:
        print "Server requests Rollback to sequence number:", dcpStream.rollback_seqno
        dcpStream = handle_rollback(dcpStream, args)
        return dcpStream

    elif dcpStream.status == ERR_NOT_SUPPORTED:
        print "Error: Stream Create Request Not Supported"
        sys.exit(1)

    else:
        print "Unhandled Stream Create Response {} {}".format(dcpStream.status, error_to_str(dcpStream.status))
        sys.exit(1)


def handleSystemEvent(response, manifest):
    # Unpack a DCP system event
    if response['event'] == EVENT_CREATE_COLLECTION:
        if response['version'] == 0:
            uid, sid, cid = struct.unpack(">QII", response['value'])
            uid = format(uid, 'x')
            sid = format(sid, 'x')
            cid = format(cid, 'x')
            print "DCP Event: vb:{}, sid:{}, what:CollectionCREATED, name:\"{}\", id:{}, scope:{}, manifest:{},"\
                  " seqno:{}".format(response['vbucket'],
                                                 response['streamId'],
                                                 response['key'],
                                                 cid,
                                                 sid,
                                                 uid,
                                                 response['by_seqno'])
            manifest['uid'] = uid
            for e in manifest['scopes']:
                if e['uid'] == sid:
                    e['collections'].append({'name':response['key'],
                                             'uid':cid});

        elif response['version'] == 1:
            uid, sid, cid, ttl = struct.unpack(">QIII", response['value'])
            uid = format(uid, 'x')
            sid = format(sid, 'x')
            cid = format(cid, 'x')
            print "DCP Event: vb:{}, sid:{}, what:CollectionCREATED, name:\"{}\", id:{}, scope:{}, ttl:{}, "\
                  "manifest:{}, seqno:{}".format(response['vbucket'],
                                                 response['streamId'],
                                                            response['key'],
                                                            cid,
                                                            sid,
                                                            ttl,
                                                            uid,
                                                            response['by_seqno'])
            manifest['uid'] = uid
            for e in manifest['scopes']:
                if e['uid'] == sid:
                    e['collections'].append({'name':response['key'],
                                             'uid': cid,
                                             'max_ttl':max_ttl});
        else:
            print "Unknown DCP Event version:", response['version']

    elif response['event'] == EVENT_DELETE_COLLECTION:
        # We can receive delete collection without a corresponding create, this
        # will happen when only the tombstone of a collection remains
        uid, sid, cid = struct.unpack(">QII", response['value'])
        uid = format(uid, 'x')
        sid = format(sid, 'x')
        cid = format(cid, 'x')
        print "DCP Event: vb:{}, sid:{}, what:CollectionDROPPED, id:{}, scope:{},  manifest:{}, "\
              "seqno:{}".format(response['vbucket'], response['streamId'], cid, sid, uid, response['by_seqno'])
        manifest['uid'] = uid
        collections = []
        for e in manifest['scopes']:
            update=False
            for c in e['collections']:
                if c['uid'] != cid:
                    collections.append(c)
                    update=True
            if update:
                e['collections'] = collections
                break

    elif response['event'] == EVENT_CREATE_SCOPE:
        uid, sid = struct.unpack(">QI", response['value'])
        uid = format(uid, 'x')
        sid = format(sid, 'x')
        print "DCP Event: vb:{}, sid:{}, what:ScopeCREATED, name:\"{}\", id:{}, manifest:{}, "\
              "seqno:{}".format(response['vbucket'],
                response['streamId'],
                                 response['key'],
                                 sid,
                                 uid,
                                 response['by_seqno'])

        # Record the scope
        manifest['uid'] = uid
        manifest['scopes'].append({'uid':sid,
                                   'name':response['key'],
                                   'collections':[]})

    elif response['event'] == EVENT_DELETE_SCOPE:
        # We can receive delete scope without a corresponding create, this
        # will happen when only the tombstone of a scope remains
        uid, sid = struct.unpack(">QI", response['value'])
        uid = format(uid, 'x')
        sid = format(sid, 'x')
        print "DCP Event: vb:{}, sid:{}, what:ScopeDROPPED, id:{}, manifest:{}, "\
              "seqno:{}".format(response['vbucket'], response['streamId'], sid, uid, response['by_seqno'])
        manifest['uid'] = uid
        scopes = []
        for e in manifest['scopes']:
            if e['uid'] != sid:
                scopes.append(e)
        manifest['scopes'] = scopes
    else:
        print "Unknown DCP Event:", response['event']
    return manifest

def handleMutation(response):
    vb = response['vbucket']
    seqno = response['by_seqno']
    action = response['opcode']
    sid = response['streamId']
    output_string = ""
    if args.keys:
        output_string += "KEY:" + response['key'] + " from collection:" + str(response['collection_id']) + ", vb:" + str(vb) + " sid:" + str(sid) + " "
    if args.docs:
        output_string += "BODY:" + response['value']
    if args.xattrs:
        if 'xattrs' in response and response['xattrs'] != None:
            output_string += " XATTRS:" + response['xattrs']
        else:
            output_string += " XATTRS: - "
    if output_string != "":
        output_string = str(DCP_Opcode_Dictionary[action]) + " -> " + output_string
        print seqno, output_string

def handleMarker(response):
    print "Snapshot Marker vb:{}, sid:{}, "\
          "start:{}, end:{}, flag:{}".format(response['vbucket'],
                                             response['streamId'],
                                             response['snap_start_seqno'],
                                             response['snap_end_seqno'],
                                             response['flag'])
    return int(response['snap_start_seqno']),int(response['snap_end_seqno'])

def checkSnapshot(vb, se, current, stream):
    if se == current:
        print "Snapshot for vb:{} has completed, end:{}, "\
              "stream.mutation_count:{}".format(vb, se, stream.mutation_count)


def process_dcp_traffic(streams, args):
    active_streams = len(streams)

    while active_streams > 0:

        for vb in streams:
            stream = vb['stream']
            if not vb['complete']:
                if stream.has_response():
                    response = stream.next_response()
                    if response == None:
                        print "No response from vbucket", vb['id']
                        vb['complete'] = True
                        active_streams -= 1
                        dcp_log_data.push_sequence_no(vb['id'])
                        continue

                    opcode = response['opcode']
                    if (opcode == CMD_MUTATION or
                        opcode == CMD_DELETION or
                        opcode == CMD_EXPIRATION):
                        handleMutation(response)
                        if args.failover_logging:
                            dcp_log_data.upsert_sequence_no(response['vbucket'],
                                                            response['by_seqno'])

                        vb['timed-out'] = args.retry_limit

                        checkSnapshot(response['vbucket'],
                                      vb['snap_end'],
                                      response['by_seqno'],
                                      stream)
                    elif opcode == CMD_SNAPSHOT_MARKER:
                        vb['snap_start'], vb['snap_end'] = handleMarker(response)
                    elif opcode == CMD_SYSTEM_EVENT:
                        vb['manifest'] = handleSystemEvent(response,
                                                           vb['manifest'])
                        checkSnapshot(response['vbucket'],
                                      vb['snap_end'],
                                      response['by_seqno'],
                                      stream)
                    elif opcode == CMD_STREAM_END:
                        print "Received stream end. Stream complete with "\
                              "reason {}.".format(response['flags'])
                        vb['complete'] = True
                        active_streams -= 1
                        dcp_log_data.push_sequence_no(response['vbucket'])
                    else:
                        print "Unexpected and unhandled opcode:{}".format(opcode)
                else:
                    print 'No response'

            if vb['complete']:
                # Second-tier timeout - after the stream close to allow other vbuckets to execute
                if vb['timed-out'] > 0:
                    vb['complete'] = False
                    active_streams += 1
                    vb['timed-out'] -= 1
                else:
                    if vb['stream_open']:
                        # Need to close stream to vb - TODO: use a function of mc client instead of raw socket
                        header = struct.pack(RES_PKT_FMT,
                                             REQ_MAGIC_BYTE,
                                             CMD_CLOSE_STREAM,
                                             0, 0, 0, vb['id'], 0, 0, 0)
                        select_dcp_client(vb['id']).s.sendall(header)
                        vb['stream_open'] = False
                        if args.stream_req_info:
                            print 'Stream to vbucket(s)', str(vb['id']), 'closed'

    # Dump each VB manifest if collections were enabled
    if args.collections:
        for vb in streams:
            print "vb:{} The following manifest state was created from the "\
                  "system events".format(vb['id'])
            print json.dumps(vb['manifest'], sort_keys=True, indent=2)


def initiate_connection(args):
    node = args.node
    bucket = args.bucket
    stream_xattrs = args.xattrs
    include_delete_times = args.delete_times
    stream_collections = args.collections
    use_compression = (args.compression > 0)
    force_compression = (args.compression > 1)
    host, port = args.node.split(":")
    host = check_valid_host(host, 'User Input')
    timeout = args.timeout

    dcp_client = DcpClient(host, int(port), timeout=timeout, do_auth=False)
    print 'Connected to:', node

    try:
        response = dcp_client.sasl_auth_plain(args.user, args.password)
    except MemcachedError as err:
        print 'ERROR:', err
        sys.exit(1)

    check_for_features(dcp_client, xattrs=stream_xattrs, collections=stream_collections, \
                       compression=use_compression)

    dcp_client.bucket_select(bucket)
    print "Successfully AUTHed to", bucket

    name = "simple_dcp_client " + str(uuid.uuid4())
    response = dcp_client.open_producer(name,
                                        xattr=stream_xattrs,
                                        delete_times=include_delete_times,
                                        collections=stream_collections)
    assert response['status'] == SUCCESS
    print "Opened DCP consumer connection"

    response = dcp_client.general_control("enable_noop", "true")
    assert response['status'] == SUCCESS
    print "Enabled NOOP"

    if args.noop_interval:
        noop_interval = str(args.noop_interval)
        response2 = dcp_client.general_control("set_noop_interval", noop_interval)
        assert response2['status'] == SUCCESS
        print "NOOP interval set to", noop_interval

    if args.opcode_dump:
        dcp_client.opcode_dump_control(True)

    if force_compression:
        response = dcp_client.general_control("force_value_compression", "true")
        assert response['status'] == SUCCESS
        print "Forcing compression on connection"

    if args.enable_expiry:
        response = dcp_client.general_control("enable_expiry_opcode", "true")
        assert response['status'] == SUCCESS
        print "Enabled Expiry Output"

    if args.enable_stream_id:
        response = dcp_client.general_control("enable_stream_id", "true")
        assert response['status'] == SUCCESS
        print "Enabled Stream-ID"

    return dcp_client


def add_streams(args):
    vb_list = args.vbuckets
    start_seq_no_list = args.start
    end_seq_no = args.end
    vb_uuid_list = args.uuid
    vb_retry = args.retry_limit
    filter_file = args.filter
    filter_json = []
    stream_collections = args.collections
    streams = []

    # Filter is a file containing JSON, it can either be a single DCP
    # stream-request value, or an array of many values. Use of many values
    # is intended to be used in conjunction with enable_stream_id and sid
    if stream_collections and filter_file != None:
        filter_file = open(args.filter, "r")
        jsonData = filter_file.read()
        parsed = json.loads(jsonData)

        # Is this an array or singular filter?
        if 'streams' in parsed:
            for f in parsed['streams']:
                filter_json.append(json.dumps(f))
        else:
            # Assume entire document is the filter
            filter_json.append(jsonData)
        print "DCP Open filter: {}".format(filter_json)
    else:
        filter_json.append('')

    for f in filter_json:
        for index in xrange(0, len(vb_list)):
            if args.stream_req_info:
                print 'Stream to vbucket', vb_list[index], 'on node' , get_node_of_dcp_client_connection(vb_list[index]), \
                    'with seq no', start_seq_no_list[index], 'and uuid', vb_uuid_list[index]
            vb = vb_list[index]
            stream = select_dcp_client(vb).stream_req(vbucket=vb,
                                                      takeover=0,
                                                      start_seqno=int(start_seq_no_list[index]),
                                                      end_seqno=end_seq_no,
                                                      vb_uuid=int(vb_uuid_list[index]),
                                                      json=f)
            handle_response = handle_stream_create_response(stream, args)
            if handle_response is None:
                vb_stream = {"id": vb_list[index],
                             "complete": False,
                             "keys_recvd": 0,
                             "timed-out": vb_retry,  # Counts the amount of times that vb_stream gets timed out
                             "stream_open": True,  # Details whether a vb stream is open to avoid repeatedly closing
                             "stream": stream,
                             # Set the manifest so we assume _default scope and collection exist
                             # KV won't replicate explicit create events for these items
                             "manifest":
                                {'scopes':[
                                    {'uid':"0", 'name':'_default', 'collections':[
                                       {'uid':"0", 'name':'_default'}]}], 'uid':0},
                             "snap_start": 0,
                             "snap_end": 0
                             }

                streams.append(vb_stream)
            else:
                for vb_stream in handle_response:
                    streams.append(vb_stream)
    return streams


def handle_rollback(dcpStream, args):
    updated_dcpStreams = []
    vb = dcpStream.vbucket
    requested_rollback_no = dcpStream.rollback_seqno

    # If argument to use JSON log files
    if args.failover_logging:
        log_fetch = dcp_log_data.get_failover_logs([vb])
        if log_fetch.get(str(vb), None) is not None:  # If the failover log is not empty, use it
            data = log_fetch[str(vb)]
            failover_values = sorted(data, key=lambda x: x[1], reverse=True)
        else:
            failover_fetch = DcpClient.get_failover_log(select_dcp_client(vb), vb)
            failover_values = failover_fetch.get('value')

    # Otherwise get failover log from server
    else:
        failover_fetch = DcpClient.get_failover_log(select_dcp_client(vb), vb)
        failover_values = failover_fetch.get('value')

    new_seq_no = []
    new_uuid = []
    failover_seq_num = 0        # Default values so that if they don't get set
    failover_vbucket_uuid = 0   # inside the for loop, 0 is used.

    for failover_log_entry in failover_values:
        if failover_log_entry[1] <= requested_rollback_no:
            failover_seq_num = failover_log_entry[1]  # closest Seq number in log
            failover_vbucket_uuid = failover_log_entry[0]  # and its UUID
            break
    new_seq_no.append(failover_seq_num)
    new_uuid.append(failover_vbucket_uuid)

    print 'Retrying stream add on vb', vb, 'with seqs', new_seq_no, 'and uuids', new_uuid
    # Input new Args for rollback stream request (separate to, but extending original args)
    temp_args = copy.deepcopy(args)
    temp_args.start = new_seq_no
    temp_args.uuid = new_uuid
    temp_args.vbuckets = [vb]

    # NOTE: This can cause continuous rollbacks making client side recursive dependent on failover logs.
    return add_streams(temp_args)


def initialise_cluster_connections(args):
    init_dcp_client = initiate_connection(args)

    config_json = json.loads(DcpClient.get_config(init_dcp_client)[2])
    global vb_map
    vb_map = config_json['vBucketServerMap']['vBucketMap']

    global dcp_client_dict
    dcp_client_dict = {}

    global dcp_log_data
    if args.log_path:
        args.log_path = os.path.normpath(args.log_path)
    dcp_log_data = LogData(args.log_path, args.vbuckets, args.keep_logs)

    # TODO: Remove globals and restructure (possibly into a class) to allow for multiple
    #       instances of the client (allowing multiple bucket connections)
    node_list = []
    for index, server in enumerate(config_json['vBucketServerMap']['serverList']):
        temp_args = copy.deepcopy(args)
        host = check_valid_host(server.split(':')[0], 'Server Config Cluster Map')
        node_list.append(host)
        port = config_json['nodesExt'][index]['services'].get('kv')

        if port is not None:
            temp_args.node = '{0}:{1}'.format(host, port)
            if 'thisNode' in config_json['nodesExt'][index]:
                dcp_client_dict[index] = {'stream': init_dcp_client,
                                          'node': temp_args.node}
            else:
                dcp_client_dict[index] = {'stream': initiate_connection(temp_args),
                                          'node': temp_args.node}


def select_dcp_client(vb):
    main_node_id = vb_map[vb][0]

    # TODO: Adding support if not my vbucket received to use replica node.
    # if len(vb_map[vb]) > 1:
    #     replica1_node_id = vb_map[vb][1]
    # if len(vb_map[vb]) > 2:
    #     replica2_node_id = vb_map[vb][2]
    # if len(vb_map[vb]) > 3:
    #     replica3_node_id = vb_map[vb][3]

    return dcp_client_dict[main_node_id]['stream']


def get_node_of_dcp_client_connection(vb):
    node_id = vb_map[vb][0]
    return dcp_client_dict[node_id]['node']


def parseArguments():
    parser = argparse.ArgumentParser(description='Create a simple DCP Consumer')
    parser.add_argument('--node', '-n', default="localhost:11210", help='Cluster Node to connect to (host:port)')
    parser.add_argument('--bucket', '-b', default="default", help='Bucket to connect to')
    parser.add_argument('--vbuckets', '-v', nargs='+', default=[0], type=int, help='vbuckets to stream')
    parser.add_argument('--start', '-s', default=['0'], nargs='+', help='start seq_num')
    parser.add_argument('--end', '-e', default=0xffffffffffffffff, type=int, help='end seq_num')
    parser.add_argument('--xattrs', '-x', help='Include Extended Attributes', default=False, action="store_true")
    parser.add_argument('--collections', '-c', help='Request Collections', default=False, action="store_true")
    parser.add_argument('--keys', '-k', help='Dump keys', default=False, action="store_true")
    parser.add_argument('--docs', '-d', help='Dump document', default=False, action="store_true")
    parser.add_argument("--filter", '-f', help="DCP Collections Filter", required=False)
    parser.add_argument("--delete_times", help="Include delete times", default=False, required=False,
                        action="store_true")
    parser.add_argument("--compression", '-y', help="Compression", required=False, action='count', default=0)
    parser.add_argument("--timeout", '-t', help="Set vbucket connection timeout length in seconds, -1 disables timeout",
                        required=False, default=5, type=float)
    parser.add_argument("--retry-limit", help="Controls the amount of times that a vb stream connection is \
    repeated without any activity (updates & deletions) before it is not retried", required=False, default=0, type=int)
    parser.add_argument("--noop-interval", help="Set time in seconds between NOOP requests", required=False)
    parser.add_argument("--opcode-dump", help="Dump all the received opcodes via print", required=False,
                        action="store_true")
    parser.add_argument("--stream-req-info", help="Display vbuckets, seq no's and uuid with every stream request",
                        required=False, action="store_true")
    parser.add_argument("--uuid", help="Set the vbucket UUID", default=['0'], nargs='+', required=False)
    parser.add_argument("--failover-logging", help="Enables use of persisted log JSON files for each vbucket, which \
    contain the failover log and sequence number", required=False, action='store_true')
    parser.add_argument("--log-path", help="Set the file path to use for the log files", default=None, required=False)
    parser.add_argument("--keep-logs", "-l", help="Retain & use current stored log files", required=False,
                        action="store_true")
    parser.add_argument("--enable-expiry", help="Trigger DCP control to allow expiry opcode messages", required=False,
                        action="store_true")
    parser.add_argument("--enable-stream-id", help="Turn on the stream-ID feature (will require use of -f)", required=False,
                        action="store_true")
    parser.add_argument("-u", "--user", help="User", required=True)
    parser.add_argument("-p", "--password", help="Password", required=True)
    parsed_args = parser.parse_args()
    if (parsed_args.log_path or parsed_args.keep_logs) and not parsed_args.failover_logging:
        parser.error("Both --log-path and --keep-logs require --failover-logging to function.")
    if not parsed_args.log_path and parsed_args.keep_logs:
        parser.error("--keep-logs requires --log-path to fetch custom logs.")
    if parsed_args.start != ['0']:
        if len(parsed_args.vbuckets) != len(parsed_args.start):
            parser.error("If multiple vbuckets are being manually set, the same number of start sequence no's "
                         "must also be set")
    if parsed_args.uuid != ['0']:
        if len(parsed_args.vbuckets) != len(parsed_args.uuid):
            parser.error("If multiple vbuckets are being manually set, the same number of uuid's "
                         "must also be set")
    if parsed_args.retry_limit and not parsed_args.timeout:
        print 'Note: It is recommended that you set a shorter vbucket timeout (via -t or --timeout) \
        when using --retry-limit'
    if parsed_args.enable_expiry and not parsed_args.delete_times:
        parsed_args.delete_times = True
        print "Note: Automatically turning on delete times due to requested usage of expiry output"
    return parsed_args


def convert_special_argument_parameters(args):
    if args.vbuckets == [-1]:
        vb_list = []
        for i in range(0, 1024):
            vb_list.append(i)
        args.vbuckets = vb_list

    if args.timeout == -1:
        args.timeout = 86400  # some very large number (one day)

    if args.start == ['0'] and len(args.vbuckets) > 1:
        args.start = ['0'] * len(args.vbuckets)

    if args.uuid == ['0'] and len(args.vbuckets) > 1:
        args.uuid = ['0'] * len(args.vbuckets)

    return args

def check_valid_host(host, errorOrigin):
    separate_host = host.split('.')
    if len(separate_host) == 4:
        for num in separate_host:
            if int(num) < 0 or int(num) > 255:
                raise IndexError("The inputted host (",
                                 host,
                                 ") has an ip address outside the standardised range. "
                                 "Error origin:", errorOrigin)
        return host
    else:
        if host == 'localhost':
            return host
        elif host == '$HOST':
            print("'$HOST' received as a stream request host input, which is invalid. "
                  "Trying to use 'localhost' instead")
            return 'localhost'
        else:
            raise StandardError("Invalid host input", host, "Error origin:", errorOrigin)


if __name__ == "__main__":
    args = parseArguments()
    args = convert_special_argument_parameters(args)
    initialise_cluster_connections(args)
    print 'Sending add vb stream request(s)'
    streams = add_streams(args)
    process_dcp_traffic(streams, args)
    print "Closing connection"
    for client_stream in dcp_client_dict.values():
        client_stream['stream'].close()
