#!/usr/bin/env python

import pprint
import time
import sys
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


def handle_stream_create_response(dcpStream):
    if dcpStream.status == SUCCESS:
        print "Stream Opened Succesfully"
    elif dcpStream.status == ERR_NOT_MY_VBUCKET:
        print "TODO: HANDLE NOT MY VBUCKET"
        vb_map = response['err_msg']
        sys.exit(1)
    elif dcpStream.status == ERR_ROLLBACK:
        print "TODO: HANDLE ROLLBACK REQUEST"
        sys.exit(1)
    elif dcpStream.status == ERR_NOT_SUPPORTED:
        print "Error: Stream Create Request Not Supported"
        sys.exit(1)
    else:
        print "Unhandled Stream Create Response", dcpStream.status
        sys.exit(1)


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
            output_string = "KEY:" + response['key']
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
    complete = False
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
                        key_count += 1
                    elif response['opcode'] == CMD_DELETION:
                        print response
                        # handleMutation(response)
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
    print "Closing connection"
    dcp_client.close()


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
    if timeout == -1:
        timeout = 86400  # some very large number (one day)
    noop_interval = str(args.noop_interval)
    global dcp_client
    dcp_client = DcpClient(host, int(port), timeout=timeout, do_auth=False)
    print 'Connected to:', node

    try:
        response = dcp_client.sasl_auth_plain(args.user, args.password)
    except MemcachedError as err:
        print err
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
    print response
    assert response['status'] == SUCCESS
    print "Opened DCP consumer connection"

    response = dcp_client.general_control("enable_noop", "true")
    assert response['status'] == SUCCESS
    print "Enabled NOOP"

    response2 = dcp_client.general_control("set_noop_interval", noop_interval)
    assert response2['status'] == SUCCESS
    print "NOOP interval set to ", noop_interval

    if force_compression:
        response = dcp_client.general_control("force_value_compression", "true")
        assert response['status'] == SUCCESS
        print "Forcing compression on connection"


def add_streams(args):
    vb_list = args.vbuckets
    start_seq_no = args.start
    end_seq_no = args.end
    streams = []
    print 'Sending add stream request(s)'
    for vb in vb_list:
        stream = dcp_client.stream_req(vbucket=int(vb), takeover=0, \
                                       start_seqno=start_seq_no, end_seqno=end_seq_no, vb_uuid=0)
        handle_stream_create_response(stream)
        vb_stream = {"id": int(vb),
                     "complete": False,
                     "keys_recvd": 0,
                     "stream": stream
                     }
        streams.append(vb_stream)
    return streams


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
    parser.add_argument("--timeout", '-t', help="Set timeout length, -1 forces persistence", required=False, default=5)
    parser.add_argument("--noop-interval", help="Set time in s between NOOP requests", required=False, default=120)
    parser.add_argument("-u", "--user", help="User", required=True)
    parser.add_argument("-p", "--password", help="Password", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parseArguments()
    initiate_connection(args)
    streams = add_streams(args)
    process_dcp_traffic(streams, args)
