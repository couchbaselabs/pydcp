
import logging
import time
import unittest

from constants import *
from uprclient import UprClient
from mcdclient import McdClient

HOST = '127.0.0.1'
PORT = 12000

def skipUnlessMcd(func):
    def _decorator(self, *args, **kwargs):
        if self.backend == RemoteServer.MCD:
            func(self, *args, **kwargs)
        else:
            logging.warning('Skpping: Requires memcached backend')
            return unittest.skip('')
    return _decorator

class RemoteServer:
    CB, DEV, MCD = range(3)

class ParametrizedTestCase(unittest.TestCase):
    """ TestCase classes that want to be parametrized should
        inherit from this class.
    """
    def __init__(self, methodName, backend, host, port):
        super(ParametrizedTestCase, self).__init__(methodName)
        self.backend = backend
        self.host = host
        self.port = port

    @staticmethod
    def parametrize(testcase_klass, backend, host, port):
        """ Create a suite containing all tests taken from the given
            subclass, passing them the parameter 'param'.
        """
        testloader = unittest.TestLoader()
        testnames = testloader.getTestCaseNames(testcase_klass)
        suite = unittest.TestSuite()
        for name in testnames:
            suite.addTest(testcase_klass(name, backend, host, port))
        return suite

class UprTestCase(ParametrizedTestCase):
    def setUp(self):
        self.upr_client = UprClient(self.host, self.port)
        self.mcd_client = McdClient(self.host, self.port)
        if (self.backend == RemoteServer.MCD):
            resp = self.mcd_client.flush().next_response()
            assert resp['status'] == SUCCESS, "Flush all is not enabled"

    def tearDown(self):
        self.upr_client.shutdown()
        self.mcd_client.shutdown()

    """Basic upr open consumer connection test

    Verifies that when the open upr consumer command is used there is a
    connection instance that is created on the server and that when the
    tcp connection is closed the connection is remove from the server"""
    def test_open_consumer_connection_command(self):
        op = self.upr_client.open_consumer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.mcd_client.stats('tap')
        response = op.next_response()
        assert response['value']['mystream:type'] == 'consumer'

        self.upr_client.shutdown()
        op = self.mcd_client.stats('tap')
        response = op.next_response()
        assert 'mystream:type' not in response['value']

    """Basic upr open producer connection test

    Verifies that when the open upr producer command is used there is a
    connection instance that is created on the server and that when the
    tcp connection is closed the connection is remove from the server"""
    def test_open_producer_connection_command(self):
        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.mcd_client.stats('tap')
        response = op.next_response()
        assert response['value']['mystream:type'] == 'producer'

        self.upr_client.shutdown()
        op = self.mcd_client.stats('tap')
        response = op.next_response()
        assert 'mystream:type' not in response['value']

    """Basic add stream test

    This test verifies a simple add stream command. It expects that a stream
    request message will be sent to the producer before a response for the
    add stream command is returned."""
    def test_add_stream_command(self):
        op = self.upr_client.open_consumer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.add_stream(0, 0)
        response = op.next_response()
        assert response['status'] == SUCCESS

    """Add stream command with no consumer vbucket

    Attempts to add a stream when no vbucket exists on the consumer. The
    client shoudl expect a not my vbucket response immediately"""
    def test_add_stream_not_my_vbucket(self):
        op = self.upr_client.open_consumer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.add_stream(1025, 0)
        response = op.next_response()
        assert response['status'] == ERR_NOT_MY_VBUCKET

    """Add stream when stream exists

    Creates a stream and then attempts to create another stream for the
    same vbucket. Expects to fail with an exists error."""
    def test_add_stream_exists(self):
        op = self.upr_client.open_consumer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.add_stream(0, 0)
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.add_stream(0, 0)
        response = op.next_response()
        assert response['status'] == ERR_KEY_EEXISTS

    def test_close_stream_command(self):
        op = self.upr_client.close_stream(0)
        response = op.next_response()
        assert response['status'] == ERR_NOT_SUPPORTED

    def test_get_failover_log_command(self):
        op = self.upr_client.get_failover_log(0)
        response = op.next_response()
        assert response['status'] == ERR_NOT_SUPPORTED

    """Basic upr stream request

    Opens a producer connection and sends a stream request command for
    vbucket 0. Since no items exist in the server we should accept the
    stream request and then send back a stream end message."""
    def test_stream_request_command(self):
        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.stream_req(0, 0, 0, 0, 0, 0)
        while op.has_response():
            response = op.next_response()
            assert response['status'] == SUCCESS

    @skipUnlessMcd
    def test_stream_request_with_ops(self):
        for i in range(10):
            op = self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)
            resp = op.next_response()
            assert resp['status'] == SUCCESS

        op = self.mcd_client.stats('vbucket-seqno')
        resp = op.next_response()
        assert resp['status'] == SUCCESS
        end_seqno = int(resp['value']['vb_0'])

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.stream_req(0, 0, 0, end_seqno, 0, 0)
        while op.has_response():
            response = op.next_response()
            assert response['status'] == SUCCESS
            print response

    @skipUnlessMcd
    def test_stream_request_with_deletes(self):
        for i in range(10):
            op = self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)
            resp = op.next_response()
            assert resp['status'] == SUCCESS

        op = self.mcd_client.stats('vbucket-seqno')
        resp = op.next_response()
        assert resp['status'] == SUCCESS
        end_seqno = int(resp['value']['vb_0'])

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.stream_req(0, 0, 0, end_seqno, 0, 0)
        while op.has_response():
            response = op.next_response()
            assert response['status'] == SUCCESS
            print response

class McdTestCase(ParametrizedTestCase):
    def setUp(self):
        self.client = McdClient(self.host, self.port)
        if (self.backend == RemoteServer.MCD):
            resp = self.client.flush().next_response()
            assert resp['status'] == SUCCESS, "Flush all is not enabled %s" % resp

    def tearDown(self):
        self.client.shutdown()

    def wait_for_stat(self, stat, val, type=''):
        for i in range(5):
            op = self.client.stats(type)
            resp = op.next_response()
            assert resp['status'] == SUCCESS
            if resp['value'][stat] == str(val):
                return True
            time.sleep(1)
        return False

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

    @skipUnlessMcd
    def test_set(self):
        op = self.client.set('key', 'value', 0, 0, 0)
        resp = op.next_response()

        op = self.client.stats()
        resp = op.next_response()
        assert resp['status'] == SUCCESS
        assert resp['value']['curr_items'] == '1'

    @skipUnlessMcd
    def test_delete(self):
        op = self.client.set('key1', 'value', 0, 0, 0)
        resp = op.next_response()
        assert resp['status'] == SUCCESS

        op = self.client.delete('key1', 0)
        resp = op.next_response()
        assert resp['status'] == SUCCESS

        assert self.wait_for_stat('curr_items', 0)
