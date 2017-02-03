#!/usr/bin/env python

import pprint
import time

from lib.dcp_bin_client import DcpClient
from lib.mc_bin_client import MemcachedClient as McdClient
from constants import *

def add_stream():
    print 'Creating DCP Client Object'
    dcp_client = DcpClient('10.142.170.101', 11210)
    
    print 'Performing SASL Auth to Bucket'
    response = dcp_client.sasl_auth_plain('all_about_that_base', '')
    
    print 'Sending open connection (consumer)'
    response = dcp_client.open_producer("haiko1980",xattr=True)
    assert response['status'] == SUCCESS
    print "Success"


    print 'Sending add stream request'
    vb = 321
    end_seq_no = 0xffffffffffffffff
    op = dcp_client.stream_req(vb, 0, 0, end_seq_no, 0, 0)

    while True:
        if op.has_response():
            response = op.next_response()
            print 'Got response: ',response['opcode']
            if response['opcode'] == CMD_STREAM_REQ:
                assert response['status'] == SUCCESS
            elif response['opcode'] == CMD_MUTATION:
                vb = response['vbucket']
                key = response['key']
                seqno =  response['value']
                xattrs = response['xattrs']
                print 'VB: %d got key %s with seqno %s' % (vb, key, xattrs)
        else: 
            print 'No response'
       
    dcp_client.shutdown()
    mcd_client.shutdown()


if __name__ == "__main__":
    add_stream()
