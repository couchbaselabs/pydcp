
import pprint
import time

from uprclient import UprClient
from mcdclient import McdClient
from constants import *

def simple_handshake_demo():
    upr_client = UprClient('127.0.0.1', 12000)
    mcd_client = McdClient('127.0.0.1', 12000)
    op = upr_client.open_producer("mystream")
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

if __name__ == "__main__":
    #simple_handshake_demo()
    add_stream_demo()
