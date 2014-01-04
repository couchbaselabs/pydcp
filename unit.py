
import logging
import time

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from constants import *
from uprclient import UprClient
from mcdclient import McdClient
from rest_client import RestClient
from statshandler import Stats

MAX_SEQNO = 0xFFFFFFFFFFFFFFFF

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

    def initialize_backend(self):
        print ''
        logging.info("-------Setup Test Case-------")
        self.rest_client = RestClient(self.host)
        if (self.backend == RemoteServer.MCD):
            self.memcached_backend_setup()
        else:
            self.couchbase_backend_setup()
        logging.info("-----Begin Test Case-----")

    def destroy_backend(self):
        logging.info("-----Tear Down Test Case-----")
        if (self.backend == RemoteServer.MCD):
            self.memcached_backend_teardown()
        else:
            self.couchbase_backend_teardown()

    def memcached_backend_setup(self):
        self.upr_client = UprClient(self.host, self.port)
        self.mcd_client = McdClient(self.host, self.port)
        resp = self.mcd_client.flush().next_response()
        assert resp['status'] == SUCCESS, "Flush all is not enabled"

    def memcached_backend_teardown(self):
        self.upr_client.shutdown()
        self.mcd_client.shutdown()

    def couchbase_backend_setup(self):
        self.rest_client = RestClient(self.host)
        for bucket in self.rest_client.get_all_buckets():
            logging.info("Deleting bucket %s" % bucket)
            assert self.rest_client.delete_bucket(bucket)
        logging.info("Creating default bucket")
        assert self.rest_client.create_default_bucket()
        Stats.wait_for_warmup(self.host, self.port)
        self.upr_client = UprClient(self.host, self.port)
        self.mcd_client = McdClient(self.host, self.port)

    def couchbase_backend_teardown(self):
        self.upr_client.shutdown()
        self.mcd_client.shutdown()
        for bucket in self.rest_client.get_all_buckets():
            logging.info("Deleting bucket %s" % bucket)
            assert self.rest_client.delete_bucket(bucket)
        self.rest_client = None

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
        self.initialize_backend()

    def tearDown(self):
        self.destroy_backend()

    """Basic upr open consumer connection test

    Verifies that when the open upr consumer command is used there is a
    connection instance that is created on the server and that when the
    tcp connection is closed the connection is remove from the server"""
    def test_open_consumer_connection_command(self):
        op = self.upr_client.open_consumer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.mcd_client.stats('upr')
        response = op.next_response()
        assert response['value']['eq_uprq:mystream:type'] == 'consumer'

        self.upr_client.shutdown()
        op = self.mcd_client.stats('upr')
        response = op.next_response()
        assert 'eq_uprq:mystream:type' not in response['value']

    """Basic upr open producer connection test

    Verifies that when the open upr producer command is used there is a
    connection instance that is created on the server and that when the
    tcp connection is closed the connection is remove from the server"""
    def test_open_producer_connection_command(self):
        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.mcd_client.stats('upr')
        response = op.next_response()
        assert response['value']['eq_uprq:mystream:type'] == 'producer'

        self.upr_client.shutdown()
        op = self.mcd_client.stats('upr')
        response = op.next_response()
        assert 'eq_uprq:mystream:type' not in response['value']

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

    """Add stream to producer

    Attempt to add stream to a producer connection. Expects to recieve
    client error response."""
    def test_add_stream_to_producer(self):

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.add_stream(0, 0)
        response = op.next_response()
        assert response['status'] == ERR_ECLIENT


    """Add stream test without open connection

    This test attempts to add a stream without idnetifying the
    client as a consumer or producer.  Excepts request
    to throw client error"""
    def test_add_stream_without_connection(self):

        op = self.upr_client.add_stream(0, 0)
        response = op.next_response()
        assert response['status'] == ERR_ECLIENT

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

    @unittest.skip("Not implemented")
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


    """Stream request with start seqno too high

    Opens a producer connection and then tries to create a stream with a seqno
    that is way too large. The stream should be closed with a range error."""
    def test_stream_request_start_seqno_too_high(self):
        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.stream_req(0, 0, MAX_SEQNO/2, MAX_SEQNO, 0, 0)
        response = op.next_response()
        assert response['status'] == ERR_ERANGE

        op = self.mcd_client.stats('upr')
        response = op.next_response()
        assert 'eq_uprq:mystream:stream_0_opaque' not in response['value']
        assert response['value']['eq_uprq:mystream:type'] == 'producer'

    """Stream request with invalid vbucket

    Opens a producer connection and then tries to create a stream with an
    invalid VBucket. Should get a not my vbucket error."""
    def test_stream_request_start_seqno_too_high(self):
        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.stream_req(1025, 0, 0, MAX_SEQNO, 0, 0)
        response = op.next_response()
        assert response['status'] == ERR_NOT_MY_VBUCKET

        op = self.mcd_client.stats('upr')
        response = op.next_response()
        assert 'eq_uprq:mystream:stream_0_opaque' not in response['value']
        assert response['value']['eq_uprq:mystream:type'] == 'producer'

    """Stream request for invalid connection

    Try to create a stream over a non-upr connection. The server should
    disconnect from the client"""
    def test_stream_request_invalid_connection(self):
        op = self.upr_client.stream_req(0, 0, 0, MAX_SEQNO, 0, 0)
        response = op.next_response()
        assert response['status'] == ERR_ECLIENT

        op = self.mcd_client.stats('upr')
        response = op.next_response()
        assert 'eq_uprq:mystream:type' not in response['value']

    """Stream request for consumer connection

    Try to create a stream on a consumer connection. The server should
    disconnect from the client"""
    def test_stream_request_consumer_connection(self):
        op = self.upr_client.open_consumer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.stream_req(0, 0, 0, MAX_SEQNO, 0, 0)
        response = op.next_response()
        assert response['status'] == ERR_ECLIENT

        op = self.mcd_client.stats('upr')
        response = op.next_response()
        assert 'eq_uprq:mystream:type' not in response['value']

    """Stream request with start seqno bigger than end seqno

    Opens a producer connection and then tries to create a stream with a start
    seqno that is bigger than the end seqno. The stream should be closed with an
    range error."""
    def test_stream_request_start_seqno_too_high(self):
        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.stream_req(0, 0, MAX_SEQNO, MAX_SEQNO/2, 0, 0)
        response = op.next_response()
        assert response['status'] == ERR_ERANGE

        op = self.mcd_client.stats('upr')
        response = op.next_response()
        assert 'eq_uprq:mystream:stream_0_opaque' not in response['value']
        assert response['value']['eq_uprq:mystream:type'] == 'producer'

    """Stream requests from the same vbucket

    Opens a stream request for a vbucket to read up to seq 100. Then sends another
    stream request for the same vbucket.  Expect a EXISTS error and upr stats
    should refer to initial created stream."""
    def test_stream_from_same_vbucket(self):

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.stream_req(0, 0, 0, MAX_SEQNO, 0, 0)
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.mcd_client.stats('upr')
        response = op.next_response()
        assert response['value']['eq_uprq:mystream:type'] == 'producer'
        created = response['value']['eq_uprq:mystream:created']
        assert created >= 0

        op = self.upr_client.stream_req(0, 0, 0, 100, 0, 0)
        response = op.next_response()
        assert response['status'] == ERR_KEY_EEXISTS

        op = self.mcd_client.stats('upr')
        response = op.next_response()
        assert response['value']['eq_uprq:mystream:created'] == created



    """Basic upr stream request (Receives mutations)

    Stores 10 items into vbucket 0 and then creates an upr stream to
    retrieve those items in order of sequence number.
    """
    def test_stream_request_with_ops(self):
        op = self.mcd_client.stop_persistence()
        resp = op.next_response()
        assert resp['status'] == SUCCESS

        for i in range(10):
            op = self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)
            resp = op.next_response()
            assert resp['status'] == SUCCESS

        op = self.mcd_client.stats('vbucket-seqno')
        resp = op.next_response()
        assert resp['status'] == SUCCESS
        end_seqno = int(resp['value']['vb_0_high_seqno'])

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        mutations = 0
        last_by_seqno = 0
        op = self.upr_client.stream_req(0, 0, 0, end_seqno, 0, 0)
        while op.has_response():
            response = op.next_response()
            assert response['status'] == SUCCESS
            if response['opcode'] == 87:
                assert response['by_seqno'] > last_by_seqno
                last_by_seqno = response['by_seqno']
                mutations = mutations + 1
        assert mutations == 10

    """Basic upr stream request (Receives mutations/deletions)

    Stores 10 items into vbucket 0 and then deletes 5 of thos items. After
    the items have been inserted/deleted from the server we create an upr
    stream to retrieve those items in order of sequence number.
    """
    def test_stream_request_with_deletes(self):
        op = self.mcd_client.stop_persistence()
        resp = op.next_response()
        assert resp['status'] == SUCCESS

        for i in range(10):
            op = self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)
            resp = op.next_response()
            assert resp['status'] == SUCCESS

        for i in range(5):
            op = self.mcd_client.delete('key' + str(i), 0)
            resp = op.next_response()
            assert resp['status'] == SUCCESS

        op = self.mcd_client.stats('vbucket-seqno')
        resp = op.next_response()
        assert resp['status'] == SUCCESS
        end_seqno = int(resp['value']['vb_0_high_seqno'])

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        mutations = 0
        deletions = 0
        last_by_seqno = 0
        op = self.upr_client.stream_req(0, 0, 0, end_seqno, 0, 0)
        while op.has_response():
            response = op.next_response()
            assert response['status'] == SUCCESS
            if response['opcode'] == 87 or response['opcode'] == 88:
                assert response['by_seqno'] > last_by_seqno
                last_by_seqno = response['by_seqno']
            if response['opcode'] == 87:
                mutations = mutations + 1
            if response['opcode'] == 88:
                deletions = deletions + 1
        assert mutations == 5
        assert deletions == 5

class McdTestCase(ParametrizedTestCase):
    def setUp(self):
        self.initialize_backend()

    def tearDown(self):
        self.destroy_backend()

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

    def test_set(self):
        op = self.client.set('key', 'value', 0, 0, 0)
        resp = op.next_response()

        op = self.client.stats()
        resp = op.next_response()
        assert resp['status'] == SUCCESS
        assert resp['value']['curr_items'] == '1'

    def test_delete(self):
        op = self.client.set('key1', 'value', 0, 0, 0)
        resp = op.next_response()
        assert resp['status'] == SUCCESS

        op = self.client.delete('key1', 0)
        resp = op.next_response()
        assert resp['status'] == SUCCESS

        assert Stats.wait_for_stat(self.client, 'curr_items', 0)

    def test_start_stop_persistence(self):
        op = self.client.stop_persistence()
        resp = op.next_response()
        assert resp['status'] == SUCCESS

        op = self.client.set('key', 'value', 0, 0, 0)
        resp = op.next_response()
        assert resp['status'] == SUCCESS

        time.sleep(2)

        op = self.client.stats()
        resp = op.next_response()
        assert resp['status'] == SUCCESS
        assert resp['value']['ep_flusher_state'] == 'paused'

        op = self.client.start_persistence()
        resp = op.next_response()
        assert resp['status'] == SUCCESS

        Stats.wait_for_persistence(self.client)
