#!/usr/bin/env python

import argparse
import copy
import json
import os
import sys

from dcp_data_persist import LogData
from lib.dcp_bin_client import DcpClient
from lib.mc_bin_client import MemcachedError
from lib.memcacheConstants import *


def check_for_features(dcp_client, xattrs=False, collections=False, compression=False):
    features = []
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
        print "Unhandled Stream Create Response", dcpStream.status
        sys.exit(1)


def handleSystemEvent(response):
    manifest, cid = struct.unpack(">QI", response['value'])
    if response['event'] == EVENT_CREATE_COLLECTION:
        print "DCP Event: Collection {} from manifest {} created at seqno: {}".format(cid, manifest, response['seqno'])
    elif response['event'] == EVENT_DELETE_COLLECTION:
        print "DCP Event: Collection {} from manifest {} deleted at seqno: {}".format(cid, manifest, response['seqno'])
    else:
        print "Unknown DCP Event:", response['event']


def handleMutation(response):
    vb = response['vbucket']
    seqno = response['by_seqno']
    output_string = ""
    if args.keys:
        output_string += "KEY:" + response['key'] + " from collection:" + str(response['collection_id']) + ", vb:" + str(vb) + " "
    if args.docs:
        output_string += "BODY:" + response['value']
    if args.xattrs:
        if 'xattrs' in response and response['xattrs'] != None:
            output_string += " XATTRS:" + response['xattrs']
        else:
            output_string += " XATTRS: - "
    if output_string != "":
        print seqno, output_string


def process_dcp_traffic(streams, args):
    key_count = 0
    active_streams = len(streams)
    while active_streams > 0:
        print "Received", str(key_count), "keys"
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
                    elif response['opcode'] == CMD_STREAM_REQ:
                        print "wasn't expecting a stream request"
                    elif response['opcode'] == CMD_MUTATION:
                        handleMutation(response)
                        if args.failover_logging:
                            dcp_log_data.upsert_sequence_no(response['vbucket'], response['by_seqno'])
                        key_count += 1
                        vb['timed-out'] = args.retry_limit
                    elif response['opcode'] == CMD_DELETION:
                        handleMutation(response)  # Printing untested with deletion, based on mutation
                        if args.failover_logging:
                            dcp_log_data.upsert_sequence_no(response['vbucket'], response['by_seqno'])
                        key_count += 1
                    elif response['opcode'] == CMD_SNAPSHOT_MARKER:
                        print "Received snapshot marker from vbucket", vb['id']
                    elif response['opcode'] == CMD_SYSTEM_EVENT:
                        handleSystemEvent(response)
                    elif response['opcode'] == CMD_STREAM_END:
                        print "Received stream end. Stream complete."
                        vb['complete'] = True
                        active_streams -= 1
                        dcp_log_data.push_sequence_no(response['vbucket'])
                    else:
                        print 'Unhandled opcode:', response['opcode']
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
    print "Ended with", key_count, "keys"


def initiate_connection(args):
    node = args.node
    bucket = args.bucket
    stream_xattrs = args.xattrs
    include_delete_times = args.delete_times
    stream_collections = args.collections
    use_compression = (args.compression > 0)
    force_compression = (args.compression > 1)
    host, port = args.node.split(":")
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

    response = dcp_client.open_producer("pydcp stream",
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

    return dcp_client


def add_streams(args):
    vb_list = args.vbuckets
    start_seq_no_list = args.start
    end_seq_no = args.end
    vb_uuid_list = args.uuid
    vb_retry = args.retry_limit
    filter_file = args.filter
    filter_json = ''
    stream_collections = args.collections
    streams = []

    if stream_collections and filter_file != None:
        filter_file = open(args.filter, "r")
        filter_json = filter_file.read()
        print "DCP Open filter: {}".format(filter_json)

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
                                                  json=filter_json)
        handle_response = handle_stream_create_response(stream, args)
        if handle_response is None:
            vb_stream = {"id": vb_list[index],
                         "complete": False,
                         "keys_recvd": 0,
                         "timed-out": vb_retry,  # Counts the amount of times that vb_stream gets timed out
                         "stream_open": True,  # Details whether a vb stream is open to avoid repeatedly closing
                         "stream": stream
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
        node = server.split(':')[0]
        host = node
        node_list.append(node)
        port = config_json['nodesExt'][index]['services'].get('kv')

        if port is not None:
            temp_args.node = '{}:{}'.format(host, port)
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
