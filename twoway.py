from uprclient import UprClient
from mcdclient import McdClient
from constants import *
import time
import logging
import pprint

def get_resp(op, text):
    print text
    print op
    response = op.next_response()
    print 'Response: %s\n' % response
    return response


def two_way_demo():
    consumer = UprClient('127.0.0.1', 5000)
    producer = UprClient('127.0.0.1', 5001)
    consumer.set_proxy(producer)
    producer.set_proxy(consumer)

    mcd_consumer = McdClient('127.0.0.1', 5000)
    mcd_producer = McdClient('127.0.0.1', 5001)

    response = get_resp(consumer.open_consumer("mystream"), 'Sending open connection (consumer)')
    assert response['status'] == SUCCESS

    response = get_resp(producer.open_producer("mystream"), 'Sending open connection (producer)')
    assert response['status'] == SUCCESS

    response = get_resp(consumer.add_stream(0, 0), 'Sending add stream request for vbucket 0')
    assert response['status'] == SUCCESS

    op = mcd_consumer.stats('upr')
    response = op.next_response()
    print response['value']
    assert response['value']['ep_upr_count'] == '1'

    op = mcd_producer.stats('upr')
    response = op.next_response()
    print response['value']
    assert response['value']['ep_upr_count'] == '1'

    get_resp(mcd_producer.set("key", "blah", 0, 0, 0), 'Create mutation on producer')

    time.sleep(3)
    get_resp(mcd_producer.delete("key", 0), 'Create mutation on producer')

    time.sleep(10)

    consumer.shutdown()
    producer.shutdown()
    mcd_consumer.shutdown()
    mcd_producer.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    two_way_demo()
