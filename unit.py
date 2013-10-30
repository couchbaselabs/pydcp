
import logging
import unittest

from constants import *
from uprclient import UprClient

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

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    suite = unittest.TestLoader().loadTestsFromTestCase(UprTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
