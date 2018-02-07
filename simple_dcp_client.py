#!/usr/bin/env python

import pprint
import time
import sys
from lib.dcp_bin_client import DcpClient
from lib.mc_bin_client import MemcachedClient as McdClient
from lib.mc_bin_client import MemcachedError

from lib.memcacheConstants import *
import argparse


def check_for_features(xattrs,collections):
    features = []
    if xattrs:
        resp = dcp_client.hello([HELO_XATTR],"pydcp feature check")
        assert HELO_XATTR in resp
    if collections:
        resp = dcp_client.hello([HELO_COLLECTIONS], "pydcp feature check")
        assert HELO_COLLECTIONS in resp


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
    else:
        print "Unhandled Stream Create Response", dcpStream.status
        sys.exit(1)


def handleSystemEvent(response):
    if response['event'] == EVENT_CREATE_COLLECTION:
        print "DCP Event: Collection {} created at seqno: {}".format(response['key'],response['seqno'])
    elif response['event'] == EVENT_DELETE_COLLECTION:
        print "DCP Event: Collection {} deleted at seqno: {}".format(response['key'],response['seqno'])
    elif response['event'] == EVENT_COLLECTION_SEPARATOR:
        print "DCP Event: Collection Separator changed to {} at seqno: {}".format(response['key'],response['seqno'])
    else:
        print "Unknown DCP Event:",response['event']


def handleMutation(response):
    vb = response['vbucket']
    seqno =  response['by_seqno']
    output_string = ""
    if args.keys:
        clen = response['collection_len']
        if clen > 0:
            print 'KEY:{0} from collection: {1}'.format(response['key'],response['key'][:clen])
        else:
            output_string = "KEY:" + response['key']
    if args.docs:
        output_string += "BODY:" + response['value']
    if args.xattrs:
        if 'xattrs' in response:
            output_string += " XATTRS:" + response['xattrs']
        else:
            output_string += " XATTRS: - "
    if output_string != "":
        print seqno, output_string


def process_dcp_traffic(stream,args):
    complete = False
    key_count = 0

    while not complete:
        print "\rReceived " + str(key_count) + " keys" ,
        sys.stdout.flush()
        if stream.has_response():
            response = stream.next_response()
            if response == None:
                print "\nNo response / Stream complete"
                complete = True
            elif response['opcode'] == CMD_STREAM_REQ:
                print "\nwasn't expecting a stream request"
            elif response['opcode'] == CMD_MUTATION:
                handleMutation(response)
                key_count += 1
            elif response['opcode'] == CMD_DELETION:
                print response
                #handleMutation(response)
                key_count += 1
            elif response['opcode'] == CMD_SNAPSHOT_MARKER:
                print "\nReceived snapshot marker"
            elif response['opcode'] == CMD_SYSTEM_EVENT:
                handleSystemEvent(response)
            else:
                print 'Unhandled opcode:',response['opcode']
        else:
            print '\nNo response'
    print "closing stream"
    dcp_client.close()

def add_stream(args):
    node = args.node
    bucket = args.bucket
    vb_list = args.vbuckets
    start_seq_no = args.start
    end_seq_no = args.end
    stream_xattrs = args.xattrs
    include_delete_times = args.delete_times
    stream_collections = args.collections
    filter_file = args.filter
    filter_json = ''
    host, port = args.node.split(":")
    global dcp_client
    dcp_client = DcpClient(host, int(port),timeout=5,do_auth=False)
    print 'Connected to:',node

    try:
        response = dcp_client.sasl_auth_plain(args.user, args.password)
    except MemcachedError as err:
        print err
        sys.exit(1)

    dcp_client.bucket_select(bucket)

    print "Successfully AUTHed to ", bucket
    check_for_features(stream_xattrs,stream_collections)
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

    print 'Sending add stream request'

    stream = dcp_client.stream_req(vbucket=int(vb_list[0]), takeover=0, \
             start_seqno=start_seq_no, end_seqno=end_seq_no, vb_uuid=0)
    handle_stream_create_response(stream)
    return stream

def parseArguments():
  parser = argparse.ArgumentParser(description='Create a simple DCP Consumer')
  parser.add_argument('--node', '-n', default="localhost:11210", help='Cluster Node to connect to (host:port)')
  parser.add_argument('--bucket', '-b', default="default", help='Bucket to connect to')
  parser.add_argument('--vbuckets', '-v',nargs='+',default=[1],  help='vbuckets to stream')
  parser.add_argument('--start', '-s',default=0, type=int, help='start seq_num')
  parser.add_argument('--end', '-e',default=0xffffffffffffffff, type=int, help='end seq_num')
  parser.add_argument('--xattrs', '-x', help='Include Extended Attributes', default=False, action="store_true")
  parser.add_argument('--collections', '-c', help='Request Collections', default=False, action="store_true")
  parser.add_argument('--keys', '-k', help='Dump keys', default=False, action="store_true")
  parser.add_argument('--docs', '-d', help='Dump document', default=False, action="store_true")
  parser.add_argument("--filter", '-f', help="DCP Filter", required=False)
  parser.add_argument("--delete_times", help="Include delete times", default=False, required=False, action="store_true")
  parser.add_argument("-u", "--user", help="User", required=True)
  parser.add_argument("-p", "--password", help="Password", required=True)
  return parser.parse_args()


if __name__ == "__main__":
    args = parseArguments()
    stream = add_stream(args)
    process_dcp_traffic(stream,args)
