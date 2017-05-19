
import pprint
import time

from uprclient import UprClient
from mcdclient import McdClient
from constants import *

import mc_bin_client
import dcp_bin_client

def simple_handshake_demo():
    upr_client = UprClient('127.0.0.1', 12000)
    mcd_client = McdClient('127.0.0.1', 12000)
    op = upr_client.open_producer("mystream", FLAG_OPEN_COLLECTIONS)
    print 'Sending open connection (producer)'
    print op
    response = op.next_response()
    print 'Response: %s\n' % response
    assert response['status'] == SUCCESS

    op = upr_client.stream_req(0, 0, 0, 0, 0, 0)
    print 'Sending Stream Request'
    print op
    while op.has_response():
        response = op.next_response()
        print 'Response: %s' % response
        assert response['status'] == SUCCESS
    print '\nGet Tap Stats\n'

    op = mcd_client.stats('tap')
    response = op.next_response()
    pprint.pprint(response['value'])

    upr_client.shutdown()
    print '\n'
    print 'Closed Upr Connection'
    print '\nGet Tap Stats\n'

    op = mcd_client.stats('tap')
    response = op.next_response()
    pprint.pprint(response['value'])

    mcd_client.shutdown()

def add_stream_demo():
    upr_client = UprClient('127.0.0.1', 12000)
    mcd_client = McdClient('127.0.0.1', 12000)
    op = upr_client.open_consumer("mystream")
    print 'Sending open connection (consumer)'
    print op
    response = op.next_response()
    print 'Response: %s\n' % response
    assert response['status'] == SUCCESS

    op = upr_client.add_stream(0, 0)
    print 'Sending add stream request'
    print op
    response = op.next_response()
    assert response['status'] == SUCCESS
    print 'Got add stream response'
    print response

    upr_client.shutdown()
    mcd_client.shutdown()

def multiple_streams(host, port):
    upr_client = UprClient(host, port)
    mcd_client = McdClient(host, port)

    op = upr_client.sasl_auth_plain('gamesim-sample', '')
    response = op.next_response()
    assert response['status'] == SUCCESS

    op = mcd_client.sasl_auth_plain('gamesim-sample', '')
    response = op.next_response()
    assert response['status'] == SUCCESS

    num_vbs = 10
    op = mcd_client.stats('vbucket-seqno')
    resp = op.next_response()
    assert resp['status'] == SUCCESS

    op = upr_client.open_producer("mystream")
    response = op.next_response()
    assert response['status'] == SUCCESS

    streams = {}
    for vb in range(num_vbs):
        en = int(resp['value']['vb_%d_high_seqno' % vb])
        op = upr_client.stream_req(vb, 0, 0, en, 0, 0)
        print "Create stream vb %d st 0 en %d" %  (vb, en)
        streams[vb] = {'op' : op,
                       'mutations' : 0,
                       'last_seqno' : 0 }

    while len(streams) > 0:
        for vb in streams.keys():
            if streams[vb]['op'].has_response():
                response = streams[vb]['op'].next_response()
                if response['opcode'] == CMD_STREAM_REQ:
                    assert response['status'] == SUCCESS
                elif response['opcode'] == CMD_SNAPSHOT_MARKER:
                    pass
                elif response['opcode'] == CMD_MUTATION:
                    assert response['by_seqno'] > streams[vb]['last_seqno']
                    streams[vb]['last_seqno'] = response['by_seqno']
                    streams[vb]['mutations'] = streams[vb]['mutations'] + 1

                    vb = response['vbucket']
                    key = response['key']
                    seqno =  response['by_seqno']
                    print 'VB: %d got key %s with seqno %d' % (vb, key, seqno)
                else:
                    del streams[vb]

    upr_client.shutdown()
    mcd_client.shutdown()

def collection_stream(host, port)


if __name__ == "__main__":
    #simple_handshake_demo()
    #add_stream_demo()
    collection_stream('127.0.0.1', 12000)
