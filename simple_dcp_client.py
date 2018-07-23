#!/usr/bin/env python

import pprint
import time
import sys
import logs
import os
from lib.dcp_bin_client import DcpClient
from lib.mc_bin_client import MemcachedClient as McdClient
from lib.mc_bin_client import MemcachedError

from lib.memcacheConstants import *
import argparse


def check_for_features(xattrs=False, collections=False, compression=False):
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
        if not args.stream_req_info:
            print "Stream Opened Succesfully"
        else:
            print 'Stream Opened Successfully on vb', dcpStream.vbucket
    elif dcpStream.status == ERR_NOT_MY_VBUCKET:
        print "TODO: HANDLE NOT MY VBUCKET"
        print dcpStream.vbucket
        sys.exit(1)
    elif dcpStream.status == ERR_ROLLBACK:
        print "ROLLBACK REQUEST RECEIVED"
        print "Server requests Rollback to sequence number:", DcpClient.get_rollback_request_no(dcp_client)
        dcpStream = handle_rollback(dcpStream, args)
    elif dcpStream.status == ERR_NOT_SUPPORTED:
        print "Error: Stream Create Request Not Supported"
        sys.exit(1)
    else:
        print "Unhandled Stream Create Response", dcpStream.status
        sys.exit(1)
    return dcpStream


def handleSystemEvent(response):
    if response['event'] == EVENT_CREATE_COLLECTION:
        print "DCP Event: Collection {} created at seqno: {}".format(response['key'], response['seqno'])
    elif response['event'] == EVENT_DELETE_COLLECTION:
        print "DCP Event: Collection {} deleted at seqno: {}".format(response['key'], response['seqno'])
    elif response['event'] == EVENT_COLLECTION_SEPARATOR:
        print "DCP Event: Collection Separator changed to {} at seqno: {}".format(response['key'], response['seqno'])
    else:
        print "Unknown DCP Event:", response['event']


def handleMutation(response):
    vb = response['vbucket']
    seqno = response['by_seqno']
    output_string = ""
    if args.keys:
        clen = response['collection_len']
        if clen > 0:
            print 'KEY:{0} from collection: {1}'.format(response['key'], response['key'][:clen])
        else:
            output_string += "KEY:" + response['key'] + ' vb ' + str(vb)
    if args.docs:
        output_string += "BODY:" + response['value']
    if args.xattrs:
        if 'xattrs' in response and response['xattrs'] != None:
            output_string += " XATTRS:" + response['xattrs']
        else:
            output_string += " XATTRS: - "
    if output_string != "":
        print seqno, output_string


def process_dcp_traffic(streams):
    key_count = 0
    active_streams = len(streams)
    while active_streams > 0:
        print "\rReceived " + str(key_count) + " keys",
        sys.stdout.flush()
        for vb in streams:
            stream = vb['stream']
            if not vb['complete']:
                if stream.has_response():
                    response = stream.next_response()
                    if response == None:
                        print "\nNo response / Stream complete"
                        vb['complete'] = True
                        active_streams -= 1
                    elif response['opcode'] == CMD_STREAM_REQ:
                        print "\nwasn't expecting a stream request"
                    elif response['opcode'] == CMD_MUTATION:
                        handleMutation(response)
                        logs.upsert_sequence_no(response['vbucket'], response['by_seqno'])
                        key_count += 1
                    elif response['opcode'] == CMD_DELETION:
                        handleMutation(response)  # Might be a bit broken
                        logs.upsert_sequence_no(response['vbucket'], response['by_seqno'])
                        key_count += 1
                    elif response['opcode'] == CMD_SNAPSHOT_MARKER:
                        print "\nReceived snapshot marker"
                    elif response['opcode'] == CMD_SYSTEM_EVENT:
                        handleSystemEvent(response)
                    elif response['opcode'] == CMD_STREAM_END:
                        print "\nReceived stream end. Stream complete."
                        vb['complete'] = True
                        active_streams -= 1
                    else:
                        print 'Unhandled opcode:', response['opcode']
                else:
                    print '\nNo response'
            if vb['complete']:
                # Need to close stream to vb - TODO: use a function of mc client instead of raw socket
                header = struct.pack(RES_PKT_FMT,
                                     REQ_MAGIC_BYTE,
                                     CMD_CLOSE_STREAM,
                                     0, 0, 0, vb['id'], 0, 0, 0)
                dcp_client.s.sendall(header)


def initiate_connection(args):
    node = args.node
    bucket = args.bucket
    stream_xattrs = args.xattrs
    include_delete_times = args.delete_times
    stream_collections = args.collections
    use_compression = (args.compression > 0)
    force_compression = (args.compression > 1)
    filter_file = args.filter
    filter_json = ''
    host, port = args.node.split(":")
    timeout = int(args.timeout)

    global dcp_client
    dcp_client = DcpClient(host, int(port), timeout=timeout, do_auth=False)
    print 'Connected to:', node

    try:
        response = dcp_client.sasl_auth_plain(args.user, args.password)
    except MemcachedError as err:
        print err, 'error'
        sys.exit(1)

    check_for_features(xattrs=stream_xattrs, collections=stream_collections, \
                       compression=use_compression)

    dcp_client.bucket_select(bucket)
    print "Successfully AUTHed to ", bucket

    if stream_collections and filter_file != None:
        filter_file = open(args.filter, "r")
        filter_json = filter_file.read()
        print "DCP Open filter: {}".format(filter_json)

    response = dcp_client.open_producer("python stream",
                                        xattr=stream_xattrs,
                                        delete_times=include_delete_times,
                                        collections=stream_collections,
                                        json=filter_json)
    assert response['status'] == SUCCESS
    print "Opened DCP consumer connection"

    response = dcp_client.general_control("enable_noop", "true")
    assert response['status'] == SUCCESS
    print "Enabled NOOP"

    if args.noop_interval:
        noop_interval = str(args.noop_interval)
        response2 = dcp_client.general_control("set_noop_interval", noop_interval)
        assert response2['status'] == SUCCESS
        print "NOOP interval set to ", noop_interval

    if args.opcode_dump:
        dcp_client.opcode_dump_control(True)

    if force_compression:
        response = dcp_client.general_control("force_value_compression", "true")
        assert response['status'] == SUCCESS
        print "Forcing compression on connection"

    reset_list = []
    if args.log_reset:
        reset_list = args.vbuckets
    else:
        for vb in args.vbuckets:
            if not os.path.exists(logs.get_path(vb)):
                reset_list.append(vb)
    logs.reset(reset_list)


def add_streams(args):
    vb_list = args.vbuckets
    start_seq_no = args.start
    end_seq_no = args.end
    vb_uuid = args.uuid
    streams = []

    print 'Sending add stream request(s)'
    if args.stream_req_info:
        print 'Stream to vbucket(s)', vb_list, 'with seq no', start_seq_no, 'and uuid', vb_uuid

    for vb in vb_list:
        stream = dcp_client.stream_req(vbucket=int(vb), takeover=0,
                                       start_seqno=start_seq_no, end_seqno=end_seq_no, vb_uuid=vb_uuid)
        handle_stream_create_response(stream, args)
        vb_stream = {"id": int(vb),
                     "complete": False,
                     "keys_recvd": 0,
                     "stream": stream
                     }
        streams.append(vb_stream)
    return streams


def handle_rollback(dcpStream, args):
    updated_dcpStreams = []

    if args.failover_log:
        f_log = open(args.failover_log, 'r')
        failover_values = []
        for line in f_log:
            line = line.replace('(', '')
            line = line.replace(')', '')
            split = line.split(',')
            value = tuple([int(split[0]), int(split[1])])
            failover_values.append(value)

        rev_failover_values = sorted(failover_values, key=lambda x: x[1])
        f_log.close()

    else:
        failover_fetch = DcpClient.get_failover_log(dcp_client, dcpStream.vbucket)
        failover_values = failover_fetch.get('value')
        rev_failover_values = failover_values[::-1]

    for row in rev_failover_values:
        server_last_seq_num = row[1]
        server_vbucket_uuid = row[0]
        args.start = server_last_seq_num
        args.uuid = server_vbucket_uuid

        print 'Retrying stream add with seq', server_last_seq_num, 'and uuid', server_vbucket_uuid
        updated_dcpStreams.insert(0, add_streams(args))
        # NOTE: This means continuous failbacks makes client side recursive.
        process_dcp_traffic(updated_dcpStreams[0])
        # instead ensures that process finishes before moving onto the next, specifically because
        # two streams with the same vbucket cannot occur.

    return updated_dcpStreams[0]


def parseArguments():
    parser = argparse.ArgumentParser(description='Create a simple DCP Consumer')
    parser.add_argument('--node', '-n', default="localhost:11210", help='Cluster Node to connect to (host:port)')
    parser.add_argument('--bucket', '-b', default="default", help='Bucket to connect to')
    parser.add_argument('--vbuckets', '-v', nargs='+', default=[0], help='vbuckets to stream')
    parser.add_argument('--start', '-s', default=0, type=int, help='start seq_num')
    parser.add_argument('--end', '-e', default=0xffffffffffffffff, type=int, help='end seq_num')
    parser.add_argument('--xattrs', '-x', help='Include Extended Attributes', default=False, action="store_true")
    parser.add_argument('--collections', '-c', help='Request Collections', default=False, action="store_true")
    parser.add_argument('--keys', '-k', help='Dump keys', default=False, action="store_true")
    parser.add_argument('--docs', '-d', help='Dump document', default=False, action="store_true")
    parser.add_argument("--filter", '-f', help="DCP Filter", required=False)
    parser.add_argument("--delete_times", help="Include delete times", default=False, required=False,
                        action="store_true")
    parser.add_argument("--compression", '-y', help="Compression", required=False, action='count', default=0)
    parser.add_argument("--timeout", '-t', help="Set timeout length in seconds, -1 disables timeout", required=False,
                        default=5)
    parser.add_argument("--noop-interval", help="Set time in seconds between NOOP requests", required=False)
    parser.add_argument("--opcode-dump", help="Dump all the received opcodes via print", required=False,
                        action="store_true")
    parser.add_argument("--stream-req-info", help="Display vbuckets, seq no's and uuid with every stream request",
                        required=False, action="store_true")
    parser.add_argument("--uuid", help="Set the vbucket UUID", type=int, default=0, required=False)
    parser.add_argument("--failover-log", help= "Option to input a custom failover log via a file path, in the form of \
    a list of tuples seperated by new lines: (uuid, seq.no) \n(uuid, seq.no)\n(uuid, seq.no)...",
                        required=False, type=str)
    parser.add_argument("--log-reset", "-l", help="Disable initial reset of log files", required=False,
                        action="store_false")
    parser.add_argument("-u", "--user", help="User", required=True)
    parser.add_argument("-p", "--password", help="Password", required=True)
    return parser.parse_args()


def argument_exceptions(args):
    if args.vbuckets == ['-1']:
        int_to_string = []
        for i in range(0,1024):
            if i not in range(170, 256):
                int_to_string.append(str(i))
        args.vbuckets = int_to_string

    if args.timeout == -1:
        args.timeout = 86400  # some very large number (one day)

    return args


if __name__ == "__main__":
    args = parseArguments()
    args = argument_exceptions(args)
    initiate_connection(args)
    streams = add_streams(args)
    process_dcp_traffic(streams)
    print "Closing connection"
    dcp_client.close()
