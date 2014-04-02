
import logging
import time
import random

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

        if host.find(':') != -1:
           self.host, self.rest_port = host.split(':')
        else:
           self.rest_port = 9000

    def initialize_backend(self):
        print ''
        logging.info("-------Setup Test Case-------")
        self.rest_client = RestClient(self.host, port=self.rest_port)
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
        self.rest_client = RestClient(self.host, port=self.rest_port)
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

class ExpTestCase(ParametrizedTestCase):
    def setUp(self):
        self.initialize_backend()

    def tearDown(self):
        self.destroy_backend()

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
        time.sleep(1)
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
        time.sleep(1)
        op = self.mcd_client.stats('upr')
        response = op.next_response()
        assert 'eq_uprq:mystream:type' not in response['value']

    def test_open_notifier_connection_command(self):
        """Basic upr open notifier connection test

        Verifies that when the open upr noifier command is used there is a
        connection instance that is created on the server and that when the
        tcp connection is closed the connection is remove from the server"""

        op = self.upr_client.open_notifier("notifier")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.mcd_client.stats('upr')
        response = op.next_response()
        assert response['value']['eq_uprq:notifier:type'] == 'notifier'

        self.upr_client.shutdown()
        time.sleep(1)

        op = self.mcd_client.stats('upr')
        response = op.next_response()
        assert 'eq_uprq:mystream:type' not in response['value']



    """Open consumer connection same key

    Verifies a single consumer connection can be opened.  Then opens a
    second consumer connection with the same key as the original.  Expects
    that the first consumer connection is closed.  Stats should reflect 1
    consumer connected
    """
    def test_open_consumer_connection_same_key(self):
        stream="mystream"
        op = self.upr_client.open_consumer(stream)
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.mcd_client.stats('upr')
        c1_stats = op.next_response()
        assert c1_stats['value']['eq_uprq:'+stream+':type'] == 'consumer'

        time.sleep(2)
        c2_stats = None
        for i in range(10):
            op = self.upr_client.open_consumer(stream)
            response = op.next_response()
            assert response['status'] == SUCCESS


            op = self.mcd_client.stats('upr')
            c2_stats = op.next_response()

        assert c2_stats is not None
        assert c2_stats['value']['eq_uprq:'+stream+':type'] == 'consumer'
        assert c2_stats['value']['ep_upr_count'] == '2'

        assert c1_stats['value']['eq_uprq:'+stream+':created'] <\
           c2_stats['value']['eq_uprq:'+stream+':created']


    """Open producer same key

    Verifies a single producer connection can be opened.  Then opens a
    second consumer connection with the same key as the original.  Expects
    that the first producer connection is closed.  Stats should reflect 1
    producer connected.
    """
    def test_open_producer_connection_same_key(self):
        stream="mystream"
        op = self.upr_client.open_producer(stream)
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.mcd_client.stats('upr')
        c1_stats = op.next_response()
        assert c1_stats['value']['eq_uprq:'+stream+':type'] == 'producer'

        time.sleep(2)
        c2_stats = None
        for i in range(10):
            op = self.upr_client.open_producer(stream)
            response = op.next_response()
            assert response['status'] == SUCCESS

            op = self.mcd_client.stats('upr')
            c2_stats = op.next_response()

        assert c2_stats['value']['eq_uprq:'+stream+':type'] == 'producer'
        assert c2_stats['value']['ep_upr_count'] == '2'

        assert c1_stats['value']['eq_uprq:'+stream+':created'] <\
           c2_stats['value']['eq_uprq:'+stream+':created']


    """ Open consumer empty name

    Tries to open a consumer connection with empty string as name.  Expects
    to recieve a client error.
    """
    def test_open_consumer_no_name(self):
        op = self.upr_client.open_consumer("")
        response = op.next_response()
        assert response['status'] == ERR_EINVAL

    """ Open producer empty name

    Tries to open a producer connection with empty string as name.  Expects
    to recieve a client error.
    """
    def test_open_producer_no_name(self):
        op = self.upr_client.open_producer("")
        response = op.next_response()
        assert response['status'] == ERR_EINVAL

    """ Open connection higher sequence number

    Use the extra's field of the open connection command to set the seqno of a
    single upr connection.  Then open another connection with a seqno higher than
    the original connection. Expects the original connections are terminiated.
    """
    @unittest.skip("seq-no's are ignored")
    def test_open_connection_higher_sequence_number(self):

        op = self.upr_client.open_consumer("mystream")
        response = op.next_response()

        for i in xrange(128):
            stream = "mystream{0}".format(i)
            op = self.upr_client.open_consumer(stream, i)
            response = op.next_response()
            assert response['status'] == SUCCESS

        op = self.mcd_client.stats('upr')
        response = op.next_response()
        assert response['value']['eq_uprq:mystream:connected'] == 'false'

    """ Open connection negative sequence number

        Use the extra's field of the open connection command and set the seqno to
        a negative value. Expects client error response.
    """
    @unittest.skip("seq-no's are ignored")
    def test_open_connection_negative_sequence_number(self):

        op = self.upr_client.open_consumer("mystream", -1)
        response = op.next_response()
        assert response['status'] != SUCCESS

    """ Open n producers and consumers

    Open n consumer and n producer connections.  Check upr stats and verify number
    of open connections = 2n with corresponding values for each conenction type.
    Expects each open connection response return true.
    """
    def test_open_n_consumer_producers(self):
        n = 1024
        ops = []
        for i in range(n):
            op = self.upr_client.open_consumer("consumer{0}".format(i))
            ops.append(op)
            op = self.upr_client.open_producer("producer{0}".format(i))
            ops.append(op)

        for op in ops:
            response = op.next_response()
            assert response['status'] == SUCCESS

        op = self.mcd_client.stats('upr')
        stats = op.next_response()
        assert stats['value']['ep_upr_count'] == str(n * 2 + 1)

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

    """Add stream to new consumer

    Creates two clients each with consumers using the same key.
    Attempts to add stream to first consumer and second consumer.
    Expects that adding stream to second consumer passes"""
    def test_add_stream_to_duplicate_consumer(self):

        op = self.upr_client.open_consumer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        upr_client2 = UprClient(self.host, self.port)
        op = upr_client2.open_consumer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.add_stream(0, 0)
        response = op.next_response()
        assert response['status'] == ERR_ECLIENT

        op = upr_client2.add_stream(0, 0)
        response = op.next_response()
        assert response['status'] == SUCCESS

    """
    Add a stream to consumer with the takeover flag set = 1.  Expects add stream
    command to return successfully.
    """
    def test_add_stream_takeover(self):

        op = self.upr_client.open_consumer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.add_stream(0, 1)
        response = op.next_response()
        assert response['status'] == SUCCESS

    """
        Open n consumer connection.  Add one stream to each consumer for the same
        vbucket.  Expects every add stream request to succeed.
    """
    def test_add_stream_n_consumers_1_stream(self):
        n = 16

        for i in xrange(n):
            op = self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)

            stream = "mystream{0}".format(i)
            op = self.upr_client.open_consumer(stream)
            response = op.next_response()
            assert response['status'] == SUCCESS

            op = self.upr_client.add_stream(0, 1)
            response = op.next_response()
            assert response['status'] == SUCCESS

        op = self.mcd_client.stats('upr')
        stats = op.next_response()
        assert stats['value']['ep_upr_count'] == str(n + 1)

    """
        Open n consumer connection.  Add n streams to each consumer for unique vbucket
        per connection. Expects every add stream request to succeed.
    """
    def test_add_stream_n_consumers_n_streams(self):
        n = 16

        vb_ids = self.all_vbucket_ids()
        for i in xrange(n):
            op = self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)

            stream = "mystream{0}".format(i)
            op = self.upr_client.open_consumer(stream)
            response = op.next_response()
            assert response['status'] == SUCCESS

            for vb in vb_ids:
                op = self.upr_client.add_stream(vb, 0)
                response = op.next_response()
                assert response['status'] == SUCCESS

        op = self.mcd_client.stats('upr')
        stats = op.next_response()
        assert stats['value']['ep_upr_count'] == str(n + 1)

    """
        Open a single consumer and add stream for all active vbuckets with the
        takeover flag set in the request.  Expects every add stream request to succeed.
    """
    def add_stream_takeover_all_vbuckets(self):

        op = self.upr_client.open_consumer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        # parsing keys: 'vb_1', 'vb_0',...
        vb_ids = self.all_vbucket_ids()
        for i in vb_ids:
            op = self.upr_client.add_stream(i, 1)
            response = op.next_response()
            assert response['status'] == SUCCESS


    """Close stream that has not been initialized.
    Expects client error."""
    def test_close_stream_command(self):
        op = self.upr_client.close_stream(0)
        response = op.next_response()
        assert response['status'] == ERR_ECLIENT


    """Close a consumer stream. Expects close operation to
    return a success."""
    def test_close_consumer_stream(self):

        op = self.upr_client.open_consumer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.add_stream(0, 0)
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.close_stream(0)
        response = op.next_response()
        assert response['status'] == SUCCESS


    """
        Open a consumer connection.  Add stream for a selected vbucket.  Then close stream.
        Immediately after closing stream send a request to add stream again.  Expects that
        stream can be added after closed.
    """
    def test_close_stream_reopen(self):
        op = self.upr_client.open_consumer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.add_stream(0, 0)
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.add_stream(0, 0)
        response = op.next_response()
        assert response['status'] == ERR_KEY_EEXISTS

        op = self.upr_client.close_stream(0)
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.add_stream(0, 0)
        response = op.next_response()
        assert response['status'] == SUCCESS

    """
        open and close stream as a consumer then takeover
        stream as producer and attempt to reopen stream
        from same vbucket
    """
    def test_close_stream_reopen_as_producer(self):
       op = self.upr_client.open_consumer("mystream")
       response = op.next_response()
       assert response['status'] == SUCCESS

       op = self.upr_client.add_stream(0, 0)
       response = op.next_response()
       assert response['status'] == SUCCESS

       op = self.upr_client.close_stream(0)
       response = op.next_response()
       assert response['status'] == SUCCESS

       op = self.upr_client.open_producer("mystream")
       response = op.next_response()
       assert response['status'] == SUCCESS

       op = self.upr_client.stream_req(0, 0, 0, 0, 0, 0)
       response = op.next_response()
       assert response['status'] == SUCCESS

       op = self.upr_client.open_consumer("mystream")
       response = op.next_response()
       assert response['status'] == SUCCESS

       op = self.upr_client.close_stream(0)
       response = op.next_response()
       assert response['status'] == ERR_KEY_ENOENT


    """Request failover log without connection

    attempts to retrieve failover log without establishing a connection to
    a producer.  Expects operation is not supported"""
    def test_get_failover_log_command(self):
        op = self.upr_client.get_failover_log(0)
        response = op.next_response()
        assert response['status'] == ERR_ECLIENT

    """Request failover log from consumer

    attempts to retrieve failover log from a consumer.  Expects
    operation is not supported."""
    def test_get_failover_log_consumer(self):

        op = self.upr_client.open_consumer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.get_failover_log(0)
        response = op.next_response()
        assert response['status'] == ERR_ECLIENT

    """Request failover log from producer

    retrieve failover log from a producer. Expects to successfully recieve
    failover log and for it to match upr stats."""
    def test_get_failover_log_producer(self):

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.get_failover_log(0)
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.mcd_client.stats('failovers')
        response = op.next_response()
        assert response['value']['failovers:vb_0:0:seq'] == '0'

    """Request failover log from invalid vbucket

    retrieve failover log from invalid vbucket. Expects to not_my_vbucket from producer."""
    def test_get_failover_invalid_vbucket(self):

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.get_failover_log(1025)
        response = op.next_response()
        assert response['status'] == ERR_NOT_MY_VBUCKET


    """Failover log during stream request

    Open a producer connection and send and add_stream request with high end_seqno.
    While waiting for end_seqno to be reached send request for failover log
    and Expects that producer is still able to return failover log
    while consumer has an open add_stream request.
    """
    def test_failover_log_during_stream_request(self):

        stream = "mystream"
        op = self.upr_client.open_producer(stream)
        response = op.next_response()
        assert response['status'] == SUCCESS

        req_op = self.upr_client.stream_req(0, 0, 0, 100, 0, 0)
        response = req_op.next_response()
        seqno = response['failover_log'][0][0]
        assert response['status'] == SUCCESS
        fail_op = self.upr_client.get_failover_log(0)
        response = fail_op.next_response()
        assert response['status'] == SUCCESS
        assert response['value'][0][0] == seqno

    """Failover log with ops

    Open a producer connection to a vbucket and start loading data to node.
    After expected number of items have been created send request for failover
    log and expect seqno to match number
    """
    def test_failover_log_with_ops(self):

        stream = "mystream"
        op = self.upr_client.open_producer(stream)
        response = op.next_response()
        assert response['status'] == SUCCESS

        req_op = self.upr_client.stream_req(0, 0, 0, 100, 0, 0)
        response = req_op.next_response()
        seqno = response['failover_log'][0][0]
        assert response['status'] == SUCCESS

        for i in range(100):
            op = self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)
            resp = op.next_response()
            assert resp['status'] == SUCCESS
            resp = req_op.next_response()

            if (i % 10) == 0:
                fail_op = self.upr_client.get_failover_log(0)
                response = fail_op.next_response()
                assert response['status'] == SUCCESS
                assert response['value'][0][0] == seqno


    """Request failover from n producers from n vbuckets

    Open n producers and attempt to fetch failover log for n vbuckets on each producer.
    Expects expects all requests for failover log to succeed and that the log for
    similar buckets match.
    """
    def test_failover_log_n_producers_n_vbuckets(self):

        n = 1024
        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        vb_ids = self.all_vbucket_ids()
        expected_seqnos = {}
        for id_ in vb_ids:
            op = self.upr_client.get_failover_log(id_)
            response = op.next_response()
            expected_seqnos[id_] = response['value'][0][0]

        for i in range(n):
            stream = "mystream{0}".format(i)
            op = self.upr_client.open_producer(stream)
            vbucket_id = vb_ids[random.randint(0,len(vb_ids) -1)]
            op = self.upr_client.get_failover_log(vbucket_id)
            response = op.next_response()
            assert response['value'][0][0] == expected_seqnos[vbucket_id]


    """Basic upr stream request

    Opens a producer connection and sends a stream request command for
    vbucket 0. Since no items exist in the server we should accept the
    stream request and then send back a stream end message."""
    @unittest.skip("Broken")
    def test_stream_request_command(self):
        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.stream_req(0, 0, 0, 0, 0, 0)
        while op.has_response():
            response = op.next_response()
            if response['opcode'] == 83:
                assert response['status'] == SUCCESS


    """Stream request with start seqno too high

    Opens a producer connection and then tries to create a stream with a seqno
    that is way too large. The stream should be closed with a range error."""
    @unittest.skip("Bug in ep-engine")
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
    def test_stream_request_invalid_vbucket(self):
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
    def test_stream_request_start_seqno_bigger_than_end_seqno(self):
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
        end_seqno = int(resp['value']['vb_0:high_seqno'])

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        mutations = 0
        last_by_seqno = 0
        op = self.upr_client.stream_req(0, 0, 0, end_seqno, 0, 0)
        while op.has_response():
            response = op.next_response()
            if response['opcode'] == 83:
                assert response['status'] == SUCCESS
            if response['opcode'] == 87:
                assert response['value'] == 'value'
                assert response['by_seqno'] > last_by_seqno
                last_by_seqno = response['by_seqno']
                mutations = mutations + 1
        assert mutations == 10

    """Receive mutation from upr stream from a later sequence

    Stores 10 items into vbucket 0 and then creates an upr stream to
    retrieve items from sequence number 7 to 10 on (4 items).
    """
    @unittest.skip("Broken")
    def test_stream_request_with_ops_start_sequence(self):
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
        end_seqno = int(resp['value']['vb_0:high_seqno'])

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.mcd_client.stats('failovers')
        resp = op.next_response()
        vb_uuid = long(resp['value']['failovers:vb_0:0:id'])
        high_seqno = long(resp['value']['failovers:vb_0:0:seq'])

        mutations = 0
        last_by_seqno = 0
        start_seqno = 7
        op = self.upr_client.stream_req(
            0, 0, start_seqno, end_seqno, vb_uuid, high_seqno)
        while op.has_response():
            response = op.next_response()
            if response['opcode'] == 83:
                assert response['status'] == SUCCESS
            if response['opcode'] == 87:
                assert response['value'] == 'value'
                assert response['by_seqno'] > last_by_seqno
                last_by_seqno = response['by_seqno']
                mutations = mutations + 1
        assert mutations == 4

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
        end_seqno = int(resp['value']['vb_0:high_seqno'])

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        mutations = 0
        deletions = 0
        last_by_seqno = 0
        op = self.upr_client.stream_req(0, 0, 0, end_seqno, 0, 0)
        while op.has_response():
            response = op.next_response()
            if response['opcode'] == 83:
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

    """Stream request that reads from disk and memory

    Insert 15,000 items and then wait for some of the checkpoints to be removed
    from memory. Then request all items starting from 0 so that we can do a disk
    backfill and then read the items that are in memory"""
    @unittest.skip("broken")
    def test_stream_request_disk_and_memory_read(self):
        for i in range(15000):
            op = self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)
            resp = op.next_response()
            assert resp['status'] == SUCCESS

        op = self.mcd_client.stats('vbucket-seqno')
        resp = op.next_response()
        assert resp['status'] == SUCCESS
        end_seqno = int(resp['value']['vb_0:high_seqno'])

        Stats.wait_for_persistence(self.mcd_client)
        assert Stats.wait_for_stat(self.mcd_client, 'vb_0:num_checkpoints', 2,
                                   'checkpoint')

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        mutations = 0
        markers = 0
        last_by_seqno = 0
        op = self.upr_client.stream_req(0, 0, 0, end_seqno, 0, 0)
        while op.has_response():
            response = op.next_response()
            if response['opcode'] == 83:
                assert response['status'] == SUCCESS
                state = Stats.get_stat(self.mcd_client,
                                       'eq_uprq:mystream:stream_0_state', 'upr')
                if state != 'dead':
                    assert state == 'backfilling'
            if response['opcode'] == 86:
                markers = markers + 1
            if response['opcode'] == 87:
                assert response['by_seqno'] > last_by_seqno
                last_by_seqno = response['by_seqno']
                mutations = mutations + 1
        assert mutations == 15000
        assert markers > 1

    """ Stream request with incremental mutations

    Insert some ops and then create a stream that wants to get more mutations
    then there are ops. The stream should pause after it gets the first set.
    Then add some more ops and wait from them to be streamed out. We will insert
    the exact amount of items that the should be streamed out."""
    def test_stream_request_incremental(self):
        for i in range(10):
            op = self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)
            resp = op.next_response()
            assert resp['status'] == SUCCESS

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        mutations = 0
        markers = 0
        last_by_seqno = 0
        streap_op = self.upr_client.stream_req(0, 0, 0, 20, 0, 0)
        while streap_op.has_response() and mutations < 10:
            response = streap_op.next_response()
            if response['opcode'] == 83:
                assert response['status'] == SUCCESS
            if response['opcode'] == 87:
                assert response['by_seqno'] > last_by_seqno
                last_by_seqno = response['by_seqno']
                mutations = mutations + 1

        for i in range(10):
            op = self.mcd_client.set('key' + str(i + 10), 'value', 0, 0, 0)
            resp = op.next_response()
            assert resp['status'] == SUCCESS

        while streap_op.has_response():
            response = streap_op.next_response()
            if response['opcode'] == 83:
                assert response['status'] == SUCCESS
            if response['opcode'] == 87:
                assert response['by_seqno'] > last_by_seqno
                last_by_seqno = response['by_seqno']
                mutations = mutations + 1

        assert mutations == 20

    """ Stream request with incremental mutations (extra ops)

    Insert some ops and then create a stream that wants to get more mutations
    then there are ops. The stream should pause after it gets the first set.
    Then add some more ops and wait from them to be streamed out. Make sure
    that we don't get more ops then we asked for since more ops were added, but
    they were past the end sequence number."""
    def test_stream_request_incremental_extra_ops(self):
        for i in range(10):
            op = self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)
            resp = op.next_response()
            assert resp['status'] == SUCCESS

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        mutations = 0
        markers = 0
        last_by_seqno = 0
        streap_op = self.upr_client.stream_req(0, 0, 0, 20, 0, 0)
        while streap_op.has_response() and mutations < 10:
            response = streap_op.next_response()
            if response['opcode'] == 83:
                assert response['status'] == SUCCESS
            if response['opcode'] == 87:
                assert response['by_seqno'] > last_by_seqno
                last_by_seqno = response['by_seqno']
                mutations = mutations + 1

        for i in range(10):
            op = self.mcd_client.set('key' + str(i + 20), 'value', 0, 0, 0)
            resp = op.next_response()
            assert resp['status'] == SUCCESS

        while streap_op.has_response():
            response = streap_op.next_response()
            if response['opcode'] == 83:
                assert response['status'] == SUCCESS
            if response['opcode'] == 87:
                assert response['by_seqno'] > last_by_seqno
                last_by_seqno = response['by_seqno']
                mutations = mutations + 1

        assert mutations == 20

    """Send stream requests for multiple

    Put some operations into four different vbucket. Then get the end sequence
    number for each vbucket and create a stream to it. Read all of the mutations
    from the streams and make sure they are all sent."""
    def test_stream_request_multiple_vbuckets(self):
        num_vbs = 4
        num_ops = 10
        for vb in range(num_vbs):
            for i in range(num_ops):
                op = self.mcd_client.set('key' + str(i), 'value', vb, 0, 0)
                resp = op.next_response()
                assert resp['status'] == SUCCESS

        op = self.mcd_client.stats('vbucket-seqno')
        resp = op.next_response()
        assert resp['status'] == SUCCESS

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        streams = {}
        for vb in range(4):
            en = int(resp['value']['vb_%d:high_seqno' % vb])
            op = self.upr_client.stream_req(vb, 0, 0, en, 0, 0)
            streams[vb] = {'op' : op,
                           'mutations' : 0,
                           'last_seqno' : 0 }

        while len(streams) > 0:
            for vb in streams.keys():
                if streams[vb]['op'].has_response():
                    response = streams[vb]['op'].next_response()
                    if response['opcode'] == 83:
                        assert response['status'] == SUCCESS
                    if response['opcode'] == 87:
                        assert response['by_seqno'] > streams[vb]['last_seqno']
                        streams[vb]['last_seqno'] = response['by_seqno']
                        streams[vb]['mutations'] = streams[vb]['mutations'] + 1
                else:
                    assert streams[vb]['mutations'] == num_ops
                    del streams[vb]

    def test_stream_request_notifier(self):
        """Open a notifier consumer and verify mutations are ready
        to be streamed"""


        op = self.upr_client.open_notifier("notifier")
        response = op.next_response()
        assert response['status'] == SUCCESS

        for i in range(100):
            op = self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)
            resp = op.next_response()
            assert resp['status'] == SUCCESS

        op = self.upr_client.stream_req(0, 0, 0, 0, 0, 100)
        response = op.next_response()
        assert response['opcode'] == CMD_STREAM_REQ
        response = op.next_response()
        assert response['opcode'] == CMD_STREAM_END


        op = self.upr_client.open_producer("producer")
        response = op.next_response()
        assert response['status'] == SUCCESS


        mutations = 0
        last_by_seqno = 0
        op = self.upr_client.stream_req(0, 0, 0, 100, 0, 100)
        while op.has_response():
            response = op.next_response()
            if response['opcode'] == 83:
                assert response['status'] == SUCCESS
            if response['opcode'] == 87:
                assert response['value'] == 'value'
                assert response['by_seqno'] > last_by_seqno
                last_by_seqno = response['by_seqno']
                mutations = mutations + 1

        assert mutations == 100


    def all_vbucket_ids(self):
        op = self.mcd_client.stats('vbucket')
        response = op.next_response()
        assert response['status'] == SUCCESS
        # parsing keys: 'vb_1', 'vb_0',...
        vb_ids = [int(v.split('_')[1]) for v in response['value'] if v != '']
        return vb_ids

class McdTestCase(ParametrizedTestCase):
    def setUp(self):
        self.initialize_backend()

    def tearDown(self):
        self.destroy_backend()

    def test_stats(self):
        op = self.mcd_client.stats()
        resp = op.next_response()
        assert resp['status'] == SUCCESS
        assert resp['value']['curr_items'] == '0'

    def test_stat_vbucket_seqno(self):
        """Tests the vbucket-seqno stat.

        Insert 10 documents and check if the sequence number has the
        correct value.
        """
        for i in range(10):
            op = self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)
            resp = op.next_response()
            assert resp['status'] == SUCCESS

        op = self.mcd_client.stats('vbucket-seqno 0')
        resp = op.next_response()
        assert resp['status'] == SUCCESS
        seqno = int(resp['value']['vb_0_high_seqno'])
        assert seqno == 10

    def test_stat_vbucket_seqno_not_my_vbucket(self):
        """Tests the vbucket-seqno NOT_MY_VBUCKET (0x07) response.

        Use a vBucket id that is way to hight in order to get a
        NOT_MY_VBUCKET (0x04) response back.
        """
        op = self.mcd_client.stats('vbucket-seqno 100000')
        resp = op.next_response()
        assert resp['status'] == ERR_NOT_MY_VBUCKET

    def test_stats_tap(self):
        op = self.mcd_client.stats('tap')
        resp = op.next_response()
        assert resp['status'] == SUCCESS
        assert resp['value']['ep_tap_backoff_period'] == '5'

    def test_set(self):
        op = self.mcd_client.set('key', 'value', 0, 0, 0)
        resp = op.next_response()

        op = self.mcd_client.stats()
        resp = op.next_response()
        assert resp['status'] == SUCCESS
        assert resp['value']['curr_items'] == '1'

    def test_delete(self):
        op = self.mcd_client.set('key1', 'value', 0, 0, 0)
        resp = op.next_response()
        assert resp['status'] == SUCCESS

        op = self.mcd_client.delete('key1', 0)
        resp = op.next_response()
        assert resp['status'] == SUCCESS

        assert Stats.wait_for_stat(self.mcd_client, 'curr_items', 0)

    def test_start_stop_persistence(self):
        retry = 5
        op = self.mcd_client.stop_persistence()
        resp = op.next_response()
        assert resp['status'] == SUCCESS

        op = self.mcd_client.set('key', 'value', 0, 0, 0)
        resp = op.next_response()
        assert resp['status'] == SUCCESS

        while retry > 0:
            time.sleep(2)

            op = self.mcd_client.stats()
            resp = op.next_response()
            assert resp['status'] == SUCCESS
            state = resp['value']['ep_flusher_state']
            if state == 'paused':
               break
            retry = retry - 1

        assert state == 'paused'
        op = self.mcd_client.start_persistence()
        resp = op.next_response()
        assert resp['status'] == SUCCESS

        Stats.wait_for_persistence(self.mcd_client)

class RebTestCase(ParametrizedTestCase):
    def __init__(self, methodName, backend, hosts, port):
        self.hosts = hosts
        super(RebTestCase, self).__init__(methodName, backend, hosts[0], port)

    def setUp(self):
        self.initialize_backend()
        self.cluster_reset()

    def tearDown(self):
        self.cluster_reset()
        self.destroy_backend()

    def cluster_reset(self, timeout = 60):
        """ rebalance out all nodes except one """

        rest = RestClient(self.host, port=self.rest_port)
        nodes = rest.get_nodes()
        if len(nodes) > 1:
            assert rest.rebalance([], self.hosts[1:])
        elif len(nodes) == 0:
            assert rest.init_self()

        assert rest.wait_for_rebalance(timeout)

    def test_mutations_during_rebalance(self):
        """verifies mutations can be streamed while cluster is rebalancing.
           during rebalance an item is set and then a stream request is made
           to get latest item along with all previous items"""

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS


        # start rebalance
        nodes = self.rest_client.get_nodes()
        assert len(nodes) == 1
        assert self.rest_client.rebalance(self.hosts[1:], [])

        # load and stream docs
        mutations = 0
        doc_count = 100
        op = self.mcd_client.stats('failovers')
        resp = op.next_response()
        vb_uuid = long(resp['value']['failovers:vb_0:0:id'])
        high_seqno = long(resp['value']['failovers:vb_0:0:seq'])

        for i in range(doc_count):
            op = self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)
            start_seqno = mutations
            mutations = mutations + 1
            op = self.upr_client.stream_req(0, 0, start_seqno, mutations, vb_uuid, high_seqno)
            last_by_seqno = 0
            while op.has_response():
                response = op.next_response()
                if response['opcode'] == 83:
                    assert response['status'] == SUCCESS
                if response['opcode'] == 87:
                    #print "%s v %s" % (response['by_seqno'], last_by_seqno)
                    assert response['by_seqno'] > last_by_seqno
                    last_by_seqno = response['by_seqno']

        assert self.rest_client.wait_for_rebalance()

