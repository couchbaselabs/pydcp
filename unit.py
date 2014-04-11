
import logging
import time
import random
import struct

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
    def __init__(self, methodName, backend, host, port, kwargs):
        super(ParametrizedTestCase, self).__init__(methodName)
        self.backend = backend
        self.host = host
        self.port = port
        self.replica = 1
        self.kwargs = kwargs

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
        assert self.rest_client.create_default_bucket(self.replica)
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
    def parametrize(testcase_klass=None, backend='cb', host='127.0.0.8091', port = 11210, **kwargs):
        """ Create a suite containing all tests taken from the given
            subclass, passing them the parameter 'param'.
        """
        assert testcase_klass is not None
        testloader = unittest.TestLoader()
        testnames = testloader.getTestCaseNames(testcase_klass)
        suite = unittest.TestSuite()

        if 'only_tc' in kwargs and kwargs['only_tc'] is not None:
            func = kwargs['only_tc']
            assert func in testnames, "TestCase not found: %s.%s" %\
                (testcase_klass.__name__, func)
            suite.addTest(testcase_klass(func, backend, host, port, kwargs))
        else:
            for name in testnames:
                suite.addTest(testcase_klass(name, backend, host, port, kwargs))
        return suite

    def all_vbucket_ids(self, type_ = None):
        vb_ids = []
        op = self.mcd_client.stats('vbucket')
        response = op.next_response()
        assert response['status'] == SUCCESS

        for vb in response['value']:
            if vb != '' and (type_ is None or response['value'][vb] == type_):
                vb_id = int(vb.split('_')[-1])
                vb_ids.append(vb_id)

        return vb_ids

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


    """
        Add stream to a consumer connection for a selected vbucket.  Start sending ops to node.
        Send close stream command to selected vbucket.  Expects that consumer has not recieved any
        subsequent mutations after producer recieved the close request.
    """
    def test_close_stream_with_ops(self):

        stream_closed = False

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS


        doc_count = 1000
        for i in range(doc_count):
            key = 'key %s' % (i)

            op = self.mcd_client.set(key, 'value', 0, 0, 0)
            response = op.next_response()
            assert response['status'] == SUCCESS


        op = self.upr_client.stream_req(0, 0, 0, doc_count, 0, 0)
        last_by_seqno = 0
        while op.has_response():

            response = op.next_response(timeout = 5)

            if response is None:
                assert stream_closed, "Error: stopped recieving data but stream wasn't closed"
                break

            assert response['opcode'] != CMD_STREAM_END, "Error: recieved all mutations on closed stream"

            if response['opcode'] == CMD_MUTATION:
                last_by_seqno = response['by_seqno']

            if not stream_closed:
                close_op = self.upr_client.close_stream(0)
                close_response = close_op.next_response()
                assert close_response['status'] == SUCCESS, 'Error: producer did not recieve close request'
                stream_closed = True

        assert last_by_seqno < doc_count, "Error: recieved all mutations on closed stream"

    """
        Sets up a consumer connection.  Adds stream and then sends 2 close stream requests.  Expects
        second request to close stream returns noent

    """
    def test_close_stream_twice(self):

        op = self.upr_client.open_producer("mystream")
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.stream_req(0, 0, 0, 1000, 0, 0)
        response = op.next_response()
        assert response['opcode'] == CMD_STREAM_REQ

        op = self.upr_client.close_stream(0)
        response = op.next_response()
        assert response['status'] == SUCCESS

        op = self.upr_client.close_stream(0)
        response = op.next_response()
        assert response['status'] == ERR_KEY_ENOENT

    """
        Test verifies that if multiple consumers are streaming from a vbucket
        that if one of the consumer closes then the producer doesn't stop
        sending changes to other consumers
    """
    def test_close_stream_n_consumers(self):

        n = 16
        for i in xrange(100):
            self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)
        Stats.wait_for_persistence(self.mcd_client)

        # add stream to be close by different client
        client2 = UprClient(self.host, self.port)
        closestream = "closestream"
        client2.open_consumer(closestream)
        client2.add_stream(0, 0)


        for i in xrange(n):

            stream = "mystream{0}".format(i)
            self.upr_client.open_consumer(stream)
            self.upr_client.add_stream(0, 1)
            if i == int(n/2):
                # close stream
                op = client2.close_stream(0)
                response = op.next_response()
                assert response['status'] == SUCCESS

        time.sleep(2)
        op = self.mcd_client.stats('upr')
        stats = op.next_response()
        key = "eq_uprq:{0}:stream_0_state".format(closestream)
        assert stats['value'][key] == 'dead'

        for i in xrange(n):
            key = "eq_uprq:mystream{0}:stream_0_state".format(i)
            assert stats['value'][key] in ('reading', 'pending')

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


    """
        Sends a stream request with start seqno greater than seqno of vbucket.  Expects
        to receive a rollback response with seqno to roll back to
    """
    def test_stream_request_rollback(self):
        op = self.upr_client.open_producer("rollback")
        response = op.next_response()
        assert response['status'] == SUCCESS

        self.mcd_client.set('key1', 'value', 0, 0, 0)
        self.mcd_client.set('key2', 'value', 0, 0, 0)

        vb_id = 'vb_0'
        vb_stats = self.mcd_client.stats('vbucket-seqno').next_response()
        fl_stats = self.mcd_client.stats('failovers').next_response()
        fail_seqno = long(fl_stats['value']['failovers:'+vb_id+':0:seq'])
        high_seqno = long(vb_stats['value'][vb_id+':high_seqno'])
        vb_uuid = long(vb_stats['value'][vb_id+':uuid'])

        op = self.upr_client.stream_req(0, 0, 1, high_seqno, vb_uuid, high_seqno)
        response = op.next_response()
        assert response['status'] == ERR_ROLLBACK
        assert response['seqno'] == fail_seqno

        start_seqno = response['seqno']
        op = self.upr_client.stream_req(0, 0, start_seqno, high_seqno,
                                        vb_uuid, high_seqno)

        last_by_seqno = 0
        while op.has_response():

            response = op.next_response()
            if response['opcode'] == CMD_MUTATION:
                last_by_seqno = response['by_seqno']

        assert last_by_seqno == high_seqno

    """
        Sends a stream request with start seqno greater than seqno of vbucket.  Expects
        to receive a rollback response with seqno to roll back to.  Instead of rolling back
        resend stream request n times each with high seqno's and expect rollback for each attempt.
    """
    def test_stream_request_n_rollbacks(self):
        op = self.upr_client.open_producer("rollback")
        response = op.next_response()
        assert response['status'] == SUCCESS

        vb_stats = self.mcd_client.stats('vbucket-seqno').next_response()
        vb_uuid = long(vb_stats['value']['vb_0:uuid'])

        for n in range(1000):
            self.mcd_client.set('key1', 'value', 0, 0, 0)

            by_seqno = n + 1
            op = self.upr_client.stream_req(0, 0, by_seqno, by_seqno+1, vb_uuid, by_seqno + 1)
            response = op.next_response()
            assert response['status'] == ERR_ROLLBACK
            assert response['seqno'] == 0

    """
        Send stream request command from n producers for the same vbucket.  Expect each request
        to succeed for each producer and verify that expected number of mutations are received
        for each request.
    """
    def test_stream_request_n_producers(self):
        clients = []

        for n in range(10):
            client = UprClient(self.host, self.port)
            op = client.open_producer("producer:%s" % n)
            response = op.next_response()
            assert response['status'] == SUCCESS
            clients.append(client)


        for n in range(10):
            self.mcd_client.set('key', 'value', 0, 0, 0)

            end_seqno = n  + 1
            for client in clients:
                op = client.stream_req(0, 0, 0, end_seqno, 0, end_seqno)
                response = op.next_response()
                assert response['status'] == SUCCESS

                # stream changes and we should reach last seqno
                # while never asked to rollback
                last_seen = 0
                while op.has_response():
                    response = op.next_response(5)
                    assert response is not None
                    assert response['opcode'] != ERR_ROLLBACK
                    if response['opcode'] == CMD_MUTATION:
                        last_seen = response['by_seqno']

                assert last_seen == end_seqno


    def test_stream_request_after_shutdown(self):
        """
        Load items from producer then shutdown producer and attempt to resume stream request
        """

        doc_count = 100
        self.upr_client.open_producer("mystream")

        for i in xrange(doc_count):
            self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)
        Stats.wait_for_persistence(self.mcd_client)

        op = self.mcd_client.stats('failovers')
        resp = op.next_response()
        vb_uuid = long(resp['value']['failovers:vb_0:0:id'])


        op = self.upr_client.stream_req(0, 0, 0, doc_count,
                                        vb_uuid, 0)
        last_by_seqno = 0
        while op.has_response():
            response = op.next_response()
            if response['opcode'] == CMD_MUTATION:
                assert response['by_seqno'] > last_by_seqno
                last_by_seqno = response['by_seqno']
            if last_by_seqno == doc_count/2:
                self.upr_client.shutdown()
                break

        self.upr_client = UprClient(self.host, self.port)
        self.upr_client.open_producer("mystream")
        op = self.upr_client.stream_req(0, 0, last_by_seqno, doc_count,
                                        vb_uuid, 0)
        while op.has_response():
            response = op.next_response()
            if response['opcode'] == CMD_MUTATION:
                # first mutation should be at location we left off
                assert response['key'] == 'key'+str(doc_count/2)
                break

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
        seqno = int(resp['value']['vb_0:high_seqno'])
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

    def setUp(self):
        self.hosts = self.kwargs.get('hosts')
        assert self.hosts is not None
        self.replica = len(self.hosts) - 1
        self.initialize_backend()
        self.cluster_reset()

    def tearDown(self):
        self.cluster_reset()
        self.destroy_backend()

    def cluster_reset(self, timeout = 600):
        """ rebalance out all nodes except one """

        rest = RestClient(self.host, port=self.rest_port)
        nodes = rest.get_nodes()
        if len(nodes) > 1:
            assert rest.rebalance([], self.hosts[1:])
        elif len(nodes) == 0:
            assert rest.init_self()

        assert rest.wait_for_rebalance(timeout)

    def mcd_reset(self, vbucket):
        """set mcd to host where vbucket is active"""

        info = self.rest_client.get_bucket_info()

        assert info is not None, 'unable to fetch vbucket map'

        host = info['vBucketServerMap']['serverList']\
                [info['vBucketServerMap']['vBucketMap'][vbucket][0]]

        assert ':' in host, 'direct port missing from serverList'

        self.host = host.split(':')[0]
        self.port = int(host.split(':')[1])
        self.mcd_client = McdClient(self.host, self.port)

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
            response = op.next_response()
            if response['status'] == ERR_NOT_MY_VBUCKET:
                time.sleep(1)
                self.mcd_reset(0)
                op = self.mcd_client.set('key' + str(i), 'value', 0, 0, 0)
                response = op.next_response()

            assert response['status'] == SUCCESS

            start_seqno = mutations
            mutations = mutations + 1
            last_by_seqno = 0
            op = self.upr_client.stream_req(0, 0, start_seqno, mutations, vb_uuid, high_seqno)

            while op.has_response():
                response = op.next_response(10)
                assert response is not None, "expected mutations to seqno: %s, last_seqno: %s" %\
                        (mutations, last_by_seqno)

                if response['opcode'] == 83:
                    assert response['status'] == SUCCESS
                if response['opcode'] == 87:
                    assert response['by_seqno'] > last_by_seqno
                    last_by_seqno = response['by_seqno']

        assert self.rest_client.wait_for_rebalance(600)

    def test_stream_during_rebalance_in_out(self):
        """rebalance in/out while streaming mutations"""

        def load(vbucket, doc_count = 100):
            self.mcd_reset(vbucket)
            for i in range(doc_count):
                key = 'key %s' % (i)
                op = self.mcd_client.set(key, 'value', vbucket, 0, 0)
                response = op.next_response()
                if response['status'] == ERR_NOT_MY_VBUCKET:
                    time.sleep(1)
                    self.mcd_reset(vbucket)
                    op = self.mcd_client.set(key, 'value', vbucket, 0, 0)
                    response = op.next_response()

                assert response['status'] == SUCCESS


        def stream(vbucket = 0, rolling_back = False):
            """ load doc_count items and stream them """
            self.mcd_reset(vbucket)

            op = self.upr_client.open_producer("mystream")
            vb_stats = self.mcd_client.stats('vbucket-seqno').next_response()
            fl_stats = self.mcd_client.stats('failovers').next_response()

            vb_id = 'vb_%s' % vbucket
            start_seqno = long(fl_stats['value']['failovers:'+vb_id+':0:seq'])
            end_seqno = long(vb_stats['value'][vb_id+':high_seqno'])
            vb_uuid = long(vb_stats['value'][vb_id+':uuid'])

            op = self.upr_client.stream_req(0, 0,
                                            start_seqno,
                                            end_seqno,
                                            vb_uuid, end_seqno)
            last_by_seqno = start_seqno
            while op.has_response():
                response = op.next_response(timeout = 5)
                assert response is not None, "response timeout"

                if response['opcode'] == CMD_STREAM_REQ:
                    if response['status'] == ERR_ROLLBACK:
                        rback_seqno = response['seqno']
                        assert rolling_back == False,\
                                 "Got unexpected response to rollback to: %s, but start_seqno: %s" %\
                                 (rback_seqno, start_seqno)
                        return stream(vbucket, rolling_back = True)
                    else:
                        assert response['status'] == SUCCESS
                if response['opcode'] == CMD_MUTATION:
                    #print "%s v %s" % (response['by_seqno'], last_by_seqno)
                    assert response['by_seqno'] > last_by_seqno
                    last_by_seqno = response['by_seqno']


        nodes = self.rest_client.get_nodes()
        assert len(nodes) == 1
        vbucket = 0

        # rebalance in
        for host in self.hosts[1:]:
            print "rebalance in: %s" % host
            assert self.rest_client.rebalance([host], [])
            load(vbucket)
            assert self.rest_client.wait_for_rebalance(600)
            stream(vbucket)

        # rebalance out

        for host in self.hosts[1:]:
            print "rebalance out: %s" % host
            assert self.rest_client.rebalance([], [host])
            load(vbucket)
            assert self.rest_client.wait_for_rebalance(600)
            stream(vbucket)

    def test_failover_swap_rebalance(self):
        """ add and failover node then perform swap rebalance """

        if len(self.hosts) <= 2:
            print "at least 3 nodes needed for this test: %s provided" % len(self.hosts)
            return True

        nodeA = self.hosts[0]
        nodeB = self.hosts[1]
        nodeC = self.hosts[2]

        # load data into each vbucket
        vb_ids = self.all_vbucket_ids()
        assert len(vb_ids) > 0
        doc_count = 100000/len(vb_ids)

        for i in range(doc_count):
            for vb in vb_ids:
                key = 'key:%s:%s' % (vb, i)
                op = self.mcd_client.set(key, 'value', vb, 0, 0)
                response = op.next_response()
                assert response['status'] == SUCCESS, "Error loading data to vb: %s" % vb


        # rebalance in nodeB
        self.rest_client.rebalance([nodeB], [])
        assert self.rest_client.wait_for_rebalance(600)

        # create new rest client
        if nodeB.find(':') != -1:
           host, rest_port = nodeB.split(':')
        else:
           host, rest_port = nodeB, 8091
        restB = RestClient(host, port = int(rest_port))

        # add nodeC
        restB.add_nodes([nodeC])

        # set nodeA to failover
        restB.failover(nodeA)

        # rebalance out nodeA
        restB.rebalance([], [nodeA])
        assert restB.wait_for_rebalance(600)

        # get bucketinfo and reset rest client in case we assert after
        bucket_info = restB.get_bucket_info()

        # verify expected seqnos of each vbid and failover table matches
        assert 'nodes' in bucket_info
        node_specs = bucket_info['nodes']
        for spec in node_specs:
            host = spec['hostname'].split(':')[0]
            port = int(spec['ports']['direct'])
            mcd_client = McdClient(host, port)
            vb_stats = mcd_client.stats('vbucket-seqno').next_response()
            assert 'value' in vb_stats
            for vb in vb_ids:
                key = 'vb_%s:high_seqno' % vb
                assert key in vb_stats['value'], "Missing stats for %s: "% key
                assert vb_stats['value'][key] == str(doc_count),\
                    "expected high_seqno: %s, got: %s" % (doc_count, vb_stats['value'][key])


        # remove nodeC before teardown
        assert restB.rebalance([], [nodeC])
        assert restB.wait_for_rebalance(600)


    def test_stream_req_during_failover(self):
        """stream_req mutations before and after failover from state-changing vbucket"""

        # start rebalance
        nodes = self.rest_client.get_nodes()
        assert len(nodes) == 1
        assert self.rest_client.rebalance(self.hosts[1:], [])
        assert self.rest_client.wait_for_rebalance(600)


        # point clients to replica vbucket
        vb_stats = self.mcd_client.stats('vbucket').next_response()
        assert 'value' in vb_stats
        replica_vbs = [key for key in vb_stats['value'].keys()\
                    if vb_stats['value'][key] == 'replica']
        assert len(replica_vbs) > 0 , 'No replica vbuckets, perhaps rebalance failed'
        vb = int(replica_vbs[0].split('_')[-1])
        self.mcd_reset(vb)

        # create a separate client for stream requests
        producer = UprClient(self.host, self.port)
        op = producer.open_producer("producerstream")

        # stream 1st item
        self.mcd_client.set('key1', 'value', vb, 0, 0)
        op = producer.stream_req(vb, 0, 0, 2, 0, 0)
        response = op.next_response(5)
        while response is not None:
            if 'key' in response:
                assert response['key'] == 'key1'
            response = op.next_response(5)

        # failover
        failover_node = self.hosts[1]
        assert self.rest_client.failover(failover_node)
        self.mcd_reset(vb)
        self.mcd_client.set('key2', 'value', vb, 0, 0)

        # update producer
        producer = UprClient(self.host, self.port)
        producer.open_producer("producerstream")
        op = producer.stream_req(vb, 0, 0, 2, 0, 0)

        # stream both items after failover
        assert self.rest_client.rebalance([], [failover_node])
        while op.has_response():
            response = op.next_response(15)
            assert response is not None, "Timeout reading stream after failover"

            if 'key' in response:
                if response['by_seqno'] == 1:
                    assert response['key'] == 'key1'
                elif response['by_seqno'] == 2:
                    assert response['key'] == 'key2'
                else:
                    assert False, "received unexpected mutation"
            if response['opcode'] == CMD_STREAM_END:
                break

        assert self.rest_client.wait_for_rebalance(600)

    def test_add_stream_during_failover(self):
        """Verify consumer stream and its data remain on non-failover node"""

        self.upr_client.open_consumer("mystream")
        assert self.rest_client.rebalance(self.hosts[1:], [])
        assert self.rest_client.wait_for_rebalance(600)

        active_vbs = self.all_vbucket_ids('active')
        replica_vbs = self.all_vbucket_ids('replica')
        assert len(active_vbs) > 0, 'No active vbuckets on node'
        assert len(replica_vbs) > 0, 'No replica vbuckets on node'

        # load data into replica of node1 by loading into node2 active vbuckets
        doc_count = 10
        for vb in replica_vbs:
            self.mcd_reset(vb)
            for i in xrange(doc_count):
                op = self.mcd_client.set('key' + str(i), 'value', vb, 0, 0)
                response = op.next_response()
                assert response['status'] == SUCCESS

        # send add_stream request to node1 replica vbuckets
        self.mcd_reset(active_vbs[0])
        for vb in replica_vbs:
            op = self.upr_client.add_stream(vb, 0)
            response = op.next_response()
            assert response['status'] == SUCCESS


        for host in self.hosts[1:]:
            assert self.rest_client.failover(host)

        assert self.rest_client.rebalance([], self.hosts[1:])
        assert self.rest_client.wait_for_rebalance(600)


        # check consumer persisted and high_seqno are correct
        time.sleep(2)
        op = self.mcd_client.stats('upr')
        stats = op.next_response()
        upr_count = stats['value']['ep_upr_count']
        assert int(upr_count) == 2,\
                "Got upr_count = {0}, expected = {1}".format(upr_count, 2)

        for vb in replica_vbs:
            key = 'eq_uprq:mystream:stream_%s_start_seqno' % vb
            assert key in stats['value'], "Stream %s missing from stats" % vb

            start_seqno = stats['value'][key]
            assert int(start_seqno) == doc_count,\
                    "Expected seqno=%s got=%s" % (doc_count, start_seqno)

        # verify data can be streamed
        self.upr_client.open_producer("producerstream")
        for vb in replica_vbs:
            op = self.upr_client.stream_req(vb, 0, 0, doc_count, 0, doc_count)
            last_by_seqno = 0
            while op.has_response():
                response = op.next_response(15)
                assert response is not None, 'Timeout receiving response from stream'

                if response['opcode'] == CMD_MUTATION:
                    assert last_by_seqno < response['by_seqno']
                    last_by_seqno = response['by_seqno']

            assert last_by_seqno == doc_count

    def test_failover_log_table_updated(self):
        """Verifies failover table entries are updated when vbucket ownership changes"""

        # get original failover table
        op = self.mcd_client.stats('failovers')
        fl_table1 = op.next_response()
        assert 'value' in fl_table1

        # rebalance in nodeB
        nodeB = self.hosts[1]
        assert self.rest_client.rebalance([nodeB], [])
        assert self.rest_client.wait_for_rebalance(600)
        replica_vbs = self.all_vbucket_ids('replica')
        assert len(replica_vbs) > 0, "No replica vbuckets!"
        self.mcd_reset(replica_vbs[0])

        # set and verify 1 item per nodeB vbucket
        set_ops = [self.mcd_client.set('key' + str(vb), 'value', vb, 0, 0)\
                                                        for vb in replica_vbs]
        assert all(map(lambda status: status == SUCCESS,\
                            [op.next_response()['status'] for op in set_ops]))

        # failover nodeB
        assert self.rest_client.failover(nodeB)
        assert self.rest_client.rebalance([], [nodeB])
        assert self.rest_client.wait_for_rebalance(600)
        self.mcd_reset(0)

        # get updated failover table
        op = self.mcd_client.stats('failovers')
        fl_table2 = op.next_response()
        assert 'value' in fl_table2

        # verify replica vbuckets have updated uuids
        # and old uuid matches uuids from original table
        for vb in replica_vbs:
            orig_uuid = long(fl_table1['value']['failovers:vb_'+str(vb)+':0:id'])
            assert orig_uuid == long(fl_table2['value']['failovers:vb_'+str(vb)+':1:id'])
            new_uuid = long(fl_table2['value']['failovers:vb_'+str(vb)+':0:id'])
            assert orig_uuid != new_uuid
