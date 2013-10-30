
import logging
import unittest

from constants import *
from uprclient import UprClient
from mcdclient import McdClient

HOST = '127.0.0.1'
PORT = 12000

class UprTestCase(unittest.TestCase):
    def setUp(self):
        self.client = UprClient(HOST, PORT)

    def tearDown(self):
        self.client.shutdown() 

    def test_open_consumer_connection_command(self):
        op = self.client.open_consumer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

    def test_open_producer_connection_command(self):
        op = self.client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

    @unittest.skip("Add stream response is broken in memcached")
    def test_add_stream_command(self):
        op = self.client.add_stream(0, 0)
        response = op.next_response()
        assert response['status'] == ERR_NOT_SUPPORTED

    def test_close_stream_command(self):
        op = self.client.close_stream(0)
        response = op.next_response()
        assert response['status'] == ERR_NOT_SUPPORTED

    def test_get_failover_log_command(self):
        op = self.client.get_failover_log(0)
        response = op.next_response()
        assert response['status'] == ERR_NOT_SUPPORTED

    @unittest.skip("Causes issue with test rerun")
    def test_stream_request_command(self):
        op = self.client.stream_req(0, 0, 0, 0, 0, 0)
        response = op.next_response()
        assert response['status'] == ERR_NOT_SUPPORTED

class McdTestCase(unittest.TestCase):
    def setUp(self):
        self.client = McdClient(HOST, PORT)

    def tearDown(self):
        self.client.shutdown()

    def test_stats(self):
        op = self.client.stats()
        resp = op.next_response()
        assert resp['status'] == SUCCESS
        assert resp['value']['curr_items'] == '0'

    def test_stats_tap(self):
        op = self.client.stats('tap')
        resp = op.next_response()
        assert resp['status'] == SUCCESS
        assert resp['value']['ep_tap_backoff_period'] == '5'

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    upr_suite = unittest.TestLoader().loadTestsFromTestCase(UprTestCase)
    mcd_suite = unittest.TestLoader().loadTestsFromTestCase(McdTestCase)
    unittest.TextTestRunner(verbosity=2).run(upr_suite)
    unittest.TextTestRunner(verbosity=2).run(mcd_suite)
