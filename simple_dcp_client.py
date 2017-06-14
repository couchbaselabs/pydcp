#!/usr/bin/env python

import pprint
import time
import sys
from lib.dcp_bin_client import DcpClient
from lib.mc_bin_client import MemcachedClient as McdClient
from lib.mc_bin_client import MemcachedError

from constants import *
import argparse


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

def handleMutation(response):
    vb = response['vbucket']
    seqno =  response['by_seqno']
    output_string = ""
    if args.keys:
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
            elif response['opcode'] == CMD_SNAPSHOT_MARKER:
                print "\nReceived snapshot marker"
            else:
                print response['opcode']
        else: 
            print '\nNo response'
    dcp_client.close()

def add_stream(args):
    node = args.node
    bucket = args.bucket
    vb_list = args.vbuckets
    start_seq_no = args.start
    end_seq_no = args.end
    stream_xattrs = args.xattrs

    global dcp_client
    dcp_client = DcpClient(node, 11210)
    print 'Connected to:',node
    
    try:
        response = dcp_client.sasl_auth_plain(bucket, '')
    except MemcachedError as err:
        print err
        sys.exit(1)
    print "Successfully AUTHed to ", bucket

    response = dcp_client.open_producer("traun",xattr=stream_xattrs)
    assert response['status'] == SUCCESS
    print "Opened DCP consumer connection"


    print 'Sending add stream request'

    stream = dcp_client.stream_req(vbucket=vb_list[0], takeover=0, \
             start_seqno=start_seq_no, end_seqno=end_seq_no, vb_uuid=0)
    handle_stream_create_response(stream)
    return stream
       
def parseArguments():
  parser = argparse.ArgumentParser(description='Create a simple DCP Consumer')
  parser.add_argument('--node', '-n', default="localhost", help='Cluster Node to connect to')
  parser.add_argument('--bucket', '-b', default="default", help='Bucket to connect to')
  parser.add_argument('--vbuckets', '-v',nargs='+',default=[1],  help='Vbuckets to stream')
  parser.add_argument('--start', '-s',default=0, type=int, help='start seq_num')
  parser.add_argument('--end', '-e',default=0xffffffffffffffff, type=int, help='end seq_num')
  parser.add_argument('--xattrs', '-x', help='Include Extended Attributes', default=False, action="store_true")
  parser.add_argument('--keys', '-k', help='Dump keys', default=False, action="store_true")
  parser.add_argument('--docs', '-d', help='Dump document', default=False, action="store_true")
  return parser.parse_args()




if __name__ == "__main__":
    args = parseArguments()
    stream = add_stream(args)
    process_dcp_traffic(stream,args)
