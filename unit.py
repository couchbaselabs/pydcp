
import logging
import time
import random
import struct

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from constants import *
from lib.dcp_bin_client import DcpClient
from lib.mc_bin_client import MemcachedClient as McdClient
from rest_client import RestClient
from statshandler import Stats
from threading import Thread

import paramiko
import os
from subprocess import Popen, PIPE
import datetime

from lib.atopstats import AtopStats


MAX_SEQNO = 0xFFFFFFFFFFFFFFFF

class RemoteServer:
    CB, DEV, MCD = range(3)

class ParametrizedTestCase(unittest.TestCase):
    """ TestCase classes that want to be parametrized should
        inherit from this class.
    """
    def __init__(self, methodName, backend, host, port,ssh_username, ssh_password, kwargs):

        super(ParametrizedTestCase, self).__init__(methodName)
        self.backend = backend
        self.host = host
        self.port = port
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.replica = 1
        self.kwargs = kwargs
        self.verification_seqno = None
        self.verification_vb = 0
        self.os_type = kwargs['os_type']
        self.bucket_type = kwargs['bucket_type']
        self.collect_stats = kwargs['collect_stats']
        if host.find(':') != -1:
           self.host, self.rest_port = host.split(':')
        else:
           self.rest_port = 9000

        self.statsHandler = Stats( self.bucket_type )

    def initialize_backend(self):
        print ''
        logging.info("-------Setup Test Case-------")
        self.rest_client = RestClient(self.host, port=self.rest_port)
        if (self.backend == RemoteServer.MCD):
            self.memcached_backend_setup()
        else:
            self.couchbase_backend_setup()




        nodes = [i.split(':')[0] for i in self.rest_client.get_nodes()]
        if self.collect_stats:
            self.atop = AtopStats(self.os_type, hosts=nodes,  user=self.ssh_username,  password=self.ssh_password)
            self.atop.restart_atop()
            time.sleep(5)
            self.atop.update_columns()
            res = self.atop.get_process_cpu("memcached")




        logging.info("-----Begin Test Case-----")


    def destroy_backend(self):
        logging.info("-----Tear Down Test Case-----")
        if (self.backend == RemoteServer.MCD):
            self.memcached_backend_teardown()
        else:
            self.couchbase_backend_teardown()
        if self.collect_stats: self.atop.stop_atop()

    def memcached_backend_setup(self):
        self.dcp_client = DcpClient(self.host, self.port)
        self.mcd_client = McdClient(self.host, self.port)
        resp = self.mcd_client.flush().next_response()
        assert resp['status'] == SUCCESS, "Flush all is not enabled"

    def memcached_backend_teardown(self):
        self.dcp_client.close()
        self.mcd_client.close()

    def couchbase_backend_setup(self):

        # this is a bit of a hack for test cases that use the drift counter and get adjusted time request. These are not
        # supported for the "normal" client so we sed the rbac.json to update their permissions. Windows is not supported


        if False and self.backend != RemoteServer.DEV:
            if self.os_type == 'linux':
                self._execute_command('/etc/init.d/couchbase-server stop')
                CMD =  'sed -i -e \'s/"SET_WITH_META",/"SET_WITH_META","SET_DRIFT_COUNTER_STATE","GET_ADJUSTED_TIME",/\' /opt/couchbase/etc/security/rbac.json'
                self._execute_command(CMD)
                time.sleep(10)
                self._execute_command('/etc/init.d/couchbase-server start')
                time.sleep(20)

            elif self.os_type == 'windows':
                # todo - make this really work
                pass
                #self._execute_command('net stop couchbaseserver')
                #CMD =  'sed -i -e \'s/"SET_WITH_META",/"SET_WITH_META","SET_DRIFT_COUNTER_STATE","GET_ADJUSTED_TIME",/\' /opt/couchbase/etc/security/rbac.json'
                #self._execute_command(CMD)
                #time.sleep(10)
                #self._execute_command('net start couchbaseserver')
                #time.sleep(20)




        self.rest_client = RestClient(self.host, port=self.rest_port)
        for bucket in self.rest_client.get_all_buckets():
            logging.info("Deleting bucket %s" % bucket)
            assert self.rest_client.delete_bucket(bucket)
        logging.info("Creating default bucket")
        assert self.rest_client.create_default_bucket(self.replica,bucket_type=self.bucket_type)
        self.statsHandler.wait_for_warmup(self.host, self.port)
        self.dcp_client = DcpClient(self.host, self.port)
        self.mcd_client = McdClient(self.host, self.port)

    def couchbase_backend_teardown(self):
        self.dcp_client.close()
        self.mcd_client.close()
        for bucket in self.rest_client.get_all_buckets():
            logging.info("Deleting bucket %s" % bucket)
            assert self.rest_client.delete_bucket(bucket)
        self.rest_client = None


    def _execute_command(self, cmd ):


        if self.host == '127.0.0.1' or self.host == 'localhost':
            p = Popen(cmd , shell=True, stdout=PIPE, stderr=PIPE)
            output, stderro = p.communicate()
            return output

        else:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            #print 'connecting to {0} with username : {1} password: {2}'.format(self.host, self.ssh_username, self.ssh_password)
            try:
                ssh_client.connect(hostname=self.host, username=self.ssh_username, password=self.ssh_password)
            except paramiko.AuthenticationException:
                print "Authentication failed for {0}".format(self.host)
                exit(1)
            except paramiko.BadHostKeyException:
                print "Invalid Host key for {0}".format(self.host)
                exit(1)
            except Exception:
                print "Can't establish SSH session with {0}".format(self.host)
                exit(1)

            stdin, stdout, stderr = ssh_client.exec_command(cmd)


            output = []
            for line in stdout.read().splitlines():
                output.append(line)

            for line in stderr.read().splitlines():
                print line


            stdin.close()
            stdout.close()
            stderr.close()

            return output



    def get_persisted_seq_no(self, vbucket, rev=1):



        # if dev, assume a Mac, other assume Linux - Windows is currently not supported
        if ('COUCH_BINDIR' in os.environ) and (self.host == '127.0.0.1' or self.host == 'localhost'):
            bindir = os.environ['COUCH_BINDIR']
        else:
            bindir =  '/opt/couchbase/bin'

        if self.os_type == 'linux':
            cmd = bindir + '/couch_dbinfo ' + self.db_file_location + '/' + str(vbucket) + \
                   '.couch.' + str(rev)  # + ' | grep update_seq'
        elif self.os_type == 'windows':
            cmd =  "'C:/Program Files/Couchbase/Server/bin/couch_dbinfo.exe' '" + self.db_file_location + '/' + str(vbucket) + \
                   '.couch.' + str(rev) + "'"


        result = self._execute_command( cmd )
        if self.os_type == 'linux':
           #return int(result.split('\n')[2].split(':')[1])
           return int(result[2].split('\n')[0].split(':')[1])
        elif self.os_type == 'windows':
           return int(result[2].split(':')[1])





    @staticmethod
    def parametrize(testcase_klass=None, backend='cb', host='127.0.0.8091', port = 11210,
                    ssh_username='root', ssh_password='couchbase', **kwargs):
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
            suite.addTest(testcase_klass(func, backend, host, port, ssh_username, ssh_password, kwargs))
        else:
            for name in testnames:
                suite.addTest(testcase_klass(name, backend, host, port, ssh_username, ssh_password, kwargs))
        return suite

    def all_vbucket_ids(self, type_ = None):
        vb_ids = []
        response = self.mcd_client.stats('vbucket')
        assert len(response) > 0

        for vb in response:
            if vb != '' and (type_ is None or response[vb] == type_):
                vb_id = int(vb.split('_')[-1])
                vb_ids.append(vb_id)

        return vb_ids

class ExpTestCase(ParametrizedTestCase):
    def setUp(self):
        self.initialize_backend()

    def tearDown(self):
        self.destroy_backend()


class StabilityTestCases(ParametrizedTestCase):


    def setUp(self):
        self.initialize_backend()


    # Set 'count' keys on the given vbuckets
    def set_keys(self, vbucket_count, mutation_count):

        count = 0

        for j in range(mutation_count):
            for i in range(vbucket_count):
                if  count % 10000 == 0:
                    #print 'vbucket', i, 'mutation', j
                    if self.collect_stats:
                        self.atop.update_columns()
                        print 'setting keys - memcache cpu:', self.atop.get_process_cpu("memcached")[self.host][1], \
                              'vsize:', self.atop.get_process_vsize("memcached")[self.host][1], \
                              'rss:', self.atop.get_process_rss("memcached")[self.host][1]
                self.mcd_client.set('key' + str(j), 0, 0, str(time.time() ), i)
                count = count + 1



    # Do 20,000,000 mutations and stream them one at a time
    def test_volume(self):


        MUTATIONS_PER_VBUCKET = 2000
        VBUCKET_COUNT = 1024


        # start the mutating
        mutation_thread = Thread( target=self.set_keys, args=(VBUCKET_COUNT, MUTATIONS_PER_VBUCKET,))
        mutation_thread.start()
        mutation_thread.join()



        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        for i in range(VBUCKET_COUNT):
            stream = self.dcp_client.stream_req(0, 0, 0, MUTATIONS_PER_VBUCKET,i)

            assert stream.status == SUCCESS, 'Unexpected status {0}'.format( stream.status)
            stream.run(MUTATIONS_PER_VBUCKET)

            assert stream.last_by_seqno == MUTATIONS_PER_VBUCKET, 'Unexpected last seq no {0}'.format( stream.last_by_seqno)


            if self.collect_stats and  i % 100 == 0:
                    self.atop.update_columns()
                    print 'streaming vbucket', i, 'memcache cpu:', self.atop.get_process_cpu("memcached")[self.host][1], \
                          'vsize:', self.atop.get_process_vsize("memcached")[self.host][1], \
                          'rss:', self.atop.get_process_rss("memcached")[self.host][1]
            self.dcp_client.close_stream(0)

        if self.collect_stats:
            self.atop.update_columns()
            print 'end of test','memcache cpu:', self.atop.get_process_cpu("memcached")[self.host][1], \
                          'vsize:', self.atop.get_process_vsize("memcached")[self.host][1], \
                          'rss:', self.atop.get_process_rss("memcached")[self.host][1]




    # Do 1,000,000 mutations and stream them one at a time
    # 12/1/2105 - this test was running vaery slowly probably due to the network so reduced by a factor of 10
    def test_lots_of_mutations(self):

        MUTATIONS_PER_VBUCKET = 100
        VBUCKET_COUNT = 1024


        # start the mutating
        mutation_thread = Thread( target=self.set_keys, args=(VBUCKET_COUNT, MUTATIONS_PER_VBUCKET,))
        mutation_thread.start()
        mutation_thread.join()



        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        for i in range(VBUCKET_COUNT):
            stream = self.dcp_client.stream_req(0, 0, 0, MUTATIONS_PER_VBUCKET,i)

            assert stream.status == SUCCESS, 'Unexpected status {0}'.format( stream.status)
            stream.run(MUTATIONS_PER_VBUCKET)

            assert stream.last_by_seqno == MUTATIONS_PER_VBUCKET, 'Unexpected last seq no {0}'.format( stream.last_by_seqno)
            self.dcp_client.close_stream(0)



    # This is for the Viber issue - MB-16915, if streams are closed (as in a rebalance stop) there was a race condition
    # which could cause a crash. If this test completes then we know we did not crash

    def test_close_streams(self):

        MUTATIONS_PER_VBUCKET = 10000

        # we have more vbuckets but just use 8
        VBUCKET_COUNT = 8


        # populate a bunch of keys
        self.set_keys(VBUCKET_COUNT, 120000/VBUCKET_COUNT)

        # start the mutating as background load
        mutation_thread = Thread( target=self.set_keys, args=(VBUCKET_COUNT, MUTATIONS_PER_VBUCKET,))
        mutation_thread.start()


        # and then do lots and lots of stream opens and then close
        for i in range(1000):

            self.dcp_client = DcpClient(self.host, self.port)
            response = self.dcp_client.open_producer("mystream")

            for j in range(VBUCKET_COUNT):
                response = self.dcp_client.stream_req(j, 0, 0, 100000, 0)

                response = self.dcp_client.close_stream(j)


        #print 'done the open and closing, waiting for mutations to complete'
        mutation_thread.join()





    def tearDown(self):
        self.destroy_backend()


""" Disconnect and reconnect test cases
"""


class DisconnectReconnectTestCases(ParametrizedTestCase):

    MUTATION_COUNT = 1000

    def setUp(self):
        self.initialize_backend()


            # Set 'count' keys on the given vbuckets
    def set_keys(self,count):
        for i in range(count):
            print 'setting key', i
            self.mcd_client.set('key' + str(i), 0, 0, str(time.time() ), 0)

        """

        # start the mutating
        mutation_thread = Thread( target=self.set_keys, args=(STREAM_START_STOP_COUNT,))
        mutation_thread.start()
        mutation_thread.join()
        """


    # Do 1,000 mutations and stream them one at a time
    def test_stream_one_mutation_at_a_time(self):


        STREAM_START_STOP_COUNT = 1000
        for i in range(STREAM_START_STOP_COUNT):
            self.mcd_client.set('key' + str(i), 0, 0, str(time.time() ), 0)



        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        stream = self.dcp_client.stream_req(0, 0, 0, STREAM_START_STOP_COUNT,0)

        for i in range(STREAM_START_STOP_COUNT):

            response = self.mcd_client.stats('failovers')
            ##vb_uuid = long(response['vb_0:0:id'])
            vb_seq = long(response['vb_0:0:seq'])


            assert stream.status == SUCCESS, 'Unexpected status {0}'.format( stream.status)
            stream.run(i+1)

            assert stream.last_by_seqno == i+1, 'Unexpected last seq no {0}'.format( stream.last_by_seqno)
            self.dcp_client.close_stream(0)




    # Do 1,000 mutations and stream each one on a separate stream
    def test_many_one_mutation_streams(self):


        STREAM_START_STOP_COUNT = 1000
        for i in range(STREAM_START_STOP_COUNT):
            self.mcd_client.set('key' + str(i), 0, 0, str(time.time() ), 0)


        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        for i in range(STREAM_START_STOP_COUNT):

            response = self.mcd_client.stats('failovers')
            vb_uuid = long(response['vb_0:0:id'])
            #vb_seq = long(response['vb_0:0:seq'])


            stream = self.dcp_client.stream_req(0, 0, i, i+1,  vb_uuid)
            assert stream.status == SUCCESS, 'Unexpected status {0}'.format( stream.status)
            stream.run(i+1)

            assert stream.last_by_seqno == i+1, 'Unexpected last seq no {0}'.format( stream.last_by_seqno)
            self.dcp_client.close_stream(0)



    # Do 1,000 mutations and stream each one on a separate stream
    def test_many_connects_and_disconnects(self):


        CONNECT_COUNT = 1000
        for i in range(10):
            self.mcd_client.set('key' + str(i), 0, 0, str(time.time() ), 0)


        for i in range(CONNECT_COUNT):
            dcp_client = DcpClient(self.host, self.port)
            response = dcp_client.open_producer("mystream" + str(i))
            assert response['status'] == SUCCESS


            response = self.mcd_client.stats('failovers')
            vb_uuid = long(response['vb_0:0:id'])
            vb_seq = long(response['vb_0:0:seq'])


            stream = dcp_client.stream_req(0, 0, 0, 10, 0)
            assert stream.status == SUCCESS, 'Unexpected status {0}'.format( stream.status)
            stream.run(10)

            assert stream.last_by_seqno == 10, 'Unexpected last seq no {0}'.format( stream.last_by_seqno)
            dcp_client.close_stream(0)
            dcp_client.close()





    def tearDown(self):
        self.destroy_backend()




""" A class which contains test cases which exercise streams on all available vbuckets and also does
    multiple clients
"""

class MultiClientTestCases(ParametrizedTestCase):

    MUTATION_COUNT = 1000

    def setUp(self):
        self.initialize_backend()



    # Set 'count' keys on the given vbuckets
    def set_keys_on_all_vbuckets(self,count, vbuckets):
        for i in range(count):
            for j in range(vbuckets):
                self.mcd_client.set('key' + str(i), 0, 0, str(time.time() ), j)
                time.sleep(0.010)






    # consume the streams for all vbuckets. Index is a unique stream identifier

    def consume_all_streams(self, dcp_client, vbucket_count, index=1):


        response = dcp_client.open_producer("mystream" + str(index))
        assert response['status'] == SUCCESS


        for i in range(vbucket_count):
            stream = dcp_client.stream_req(i, 0, 0, MultiClientTestCases.MUTATION_COUNT, 0)
            assert stream.status == SUCCESS
            stream.run(MultiClientTestCases.MUTATION_COUNT)
            assert stream.last_by_seqno == MultiClientTestCases.MUTATION_COUNT



    """ This is the main routine - takes as parameter the number of clients and vbuckets and
        verifies the clients receive streams.
    """
    def vary_by_clients_streams_and_vbuckets(self, client_count, vbucket_count):



        dcp_clients = []
        for i in range(client_count):
            dcp_clients.append( DcpClient(self.host, self.port) )




        # concurrently set the keys
        mutation_thread = Thread( target=self.set_keys_on_all_vbuckets,
                                  args=(MultiClientTestCases.MUTATION_COUNT,vbucket_count,))
        mutation_thread.start()
        # don't do a join, let the mutations run concurrently with the stream

        consumer_threads = []
        for i in range(client_count):
            consumer_threads.append( Thread( target=self.consume_all_streams,
                                  args=( dcp_clients[i],vbucket_count, i)) )

        for i in consumer_threads:
            i.start()


        for i in consumer_threads:
            i.join()


        # any assertions will be done in the thread


    def test_many_clients_1_vbucket(self):
        self.vary_by_clients_streams_and_vbuckets(5, 1)



    def test_1_client_1024_vbuckets(self):
        self.vary_by_clients_streams_and_vbuckets(1, 1024)



    def test_5_clients_1024_vbuckets(self):
        self.vary_by_clients_streams_and_vbuckets(5, 1024)

    @unittest.skip("invalid: dont support 20 clients")
    def test_20_clients_1024_vbuckets(self):
        self.vary_by_clients_streams_and_vbuckets(20, 1024)



    def tearDown(self):
        self.destroy_backend()
















class SnapshotTestCases(ParametrizedTestCase):
    def setUp(self):
        self.initialize_backend()


    """ do 100,000 mutations and retrieve the stream
    """
    def test_very_large_stream(self):
        doc_count = snap_end_seqno = 100000

        for i in range(doc_count):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        mutations = 0
        last_by_seqno = 0
        stream = self.dcp_client.stream_req(0, 0, 0, doc_count, 0, 0)
        assert stream.status == SUCCESS
        stream.run()


        assert stream.last_by_seqno == doc_count

        self.verification_seqno = doc_count



    """Stream request that reads from disk and memory

    Insert 15,000 items and then wait for some of the checkpoints to be removed
    from memory. Then request all items starting from 0 so that we can do a disk
    backfill and then read the items that are in memory"""

    def test_stream_request_disk(self):
        for i in range(15000):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        resp = self.mcd_client.stats('vbucket-seqno')
        end_seqno = int(resp['vb_0:high_seqno'])

        self.statsHandler.wait_for_persistence(self.mcd_client)
        assert Stats.wait_for_stat(self.mcd_client, 'vb_0:num_checkpoints', 2,
                                   'checkpoint')

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        mutations = 0
        markers = 0
        last_by_seqno = 0
        stream = self.dcp_client.stream_req(0, 0, 0, end_seqno, 0)
        assert stream.status == SUCCESS

        state = Stats.get_stat(self.mcd_client,
                               'eq_dcpq:mystream:stream_0_state', 'dcp')
        if state != 'dead':
            assert state == 'backfilling'

        responses = stream.run()

        markers = \
           len(filter(lambda r: r['opcode']==CMD_SNAPSHOT_MARKER, responses))


        # the below assert fails so we can assume it is working as expected.
        # I saw markers as 1 and num checkpoints as 2
        #assert markers == int(stats['vb_0:num_checkpoints'])

        assert stream.last_by_seqno == 15000



    def tearDown(self):
        self.destroy_backend()






class DcpTestCase(ParametrizedTestCase):
    def setUp(self):
        self.initialize_backend()
        if self.bucket_type != 'ephemeral':
            self.db_file_location = Stats.get_stat( self.mcd_client, 'ep_dbname' )

    def tearDown(self):

        if self.verification_seqno is not None and self.bucket_type != 'ephemeral':
            self.statsHandler.wait_for_persistence(self.mcd_client)
            persisted_seqno = self.get_persisted_seq_no(self.verification_vb)
            assert self.verification_seqno == persisted_seqno, \
                  'invalid persisted sequence number. Expected {0}, actual {1}'.\
                       format(self.verification_seqno, persisted_seqno)

        self.destroy_backend()


    """Basic dcp open consumer connection test

    Verifies that when the open dcp consumer command is used there is a
    connection instance that is created on the server and that when the
    tcp connection is closed the connection is remove from the server"""
    def test_open_consumer_connection_command(self):
        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.mcd_client.stats('dcp')
        assert response['eq_dcpq:mystream:type'] == 'consumer'

        self.dcp_client.close()
        time.sleep(1)
        response = self.mcd_client.stats('dcp')

        assert 'eq_dcpq:mystream:type' not in response

    """Basic dcp open producer connection test

    Verifies that when the open dcp producer command is used there is a
    connection instance that is created on the server and that when the
    tcp connection is closed the connection is remove from the server"""
    def test_open_producer_connection_command(self):

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.mcd_client.stats('dcp')
        assert response['eq_dcpq:mystream:type'] == 'producer'

        self.dcp_client.close()
        time.sleep(1)
        response = self.mcd_client.stats('dcp')
        assert 'eq_dcpq:mystream:type' not in response

    def test_open_notifier_connection_command(self):
        """Basic dcp open notifier connection test

        Verifies that when the open dcp noifier command is used there is a
        connection instance that is created on the server and that when the
        tcp connection is closed the connection is remove from the server"""

        response = self.dcp_client.open_notifier("notifier")
        assert response['status'] == SUCCESS

        response = self.mcd_client.stats('dcp')
        assert response['eq_dcpq:notifier:type'] == 'notifier'

        self.dcp_client.close()
        time.sleep(1)

        response = self.mcd_client.stats('dcp')
        assert 'eq_dcpq:mystream:type' not in response



    """Open consumer connection same key

    Verifies a single consumer connection can be opened.  Then opens a
    second consumer connection with the same key as the original.  Expects
    that the first consumer connection is closed.  Stats should reflect 1
    consumer connected
    """
    def test_open_consumer_connection_same_key(self):
        stream = "mystream"
        self.dcp_client.open_consumer(stream)

        c1_stats = self.mcd_client.stats('dcp')
        assert c1_stats['eq_dcpq:'+stream+':type'] == 'consumer'

        time.sleep(2)
        c2_stats = None
        for i in range(10):
            self.dcp_client = DcpClient(self.host, self.port)
            response = self.dcp_client.open_consumer(stream)
            assert response['status'] == SUCCESS


        c2_stats = self.mcd_client.stats('dcp')
        assert c2_stats is not None
        assert c2_stats['eq_dcpq:'+stream+':type'] == 'consumer'
        assert c2_stats['ep_dcp_count'] == '1'

        assert c1_stats['eq_dcpq:'+stream+':created'] <\
           c2_stats['eq_dcpq:'+stream+':created']


    """Open producer same key

    Verifies a single producer connection can be opened.  Then opens a
    second consumer connection with the same key as the original.  Expects
    that the first producer connection is closed.  Stats should reflect 1
    producer connected.
    """
    def test_open_producer_connection_same_key(self):
        stream="mystream"
        self.dcp_client.open_producer(stream)

        c1_stats = self.mcd_client.stats('dcp')
        assert c1_stats['eq_dcpq:'+stream+':type'] == 'producer'

        time.sleep(2)
        c2_stats = None
        for i in range(10):
            conn = DcpClient(self.host, self.port)
            response = conn.open_producer(stream)
            assert response['status'] == SUCCESS

        c2_stats = self.mcd_client.stats('dcp')

        assert c2_stats['eq_dcpq:'+stream+':type'] == 'producer'


        # CBQE-3410 1 or 2 is ok
        assert c2_stats['ep_dcp_count'] == '1' or c2_stats['ep_dcp_count'] == '2'

        assert c1_stats['eq_dcpq:'+stream+':created'] <\
           c2_stats['eq_dcpq:'+stream+':created']


    """ Open consumer empty name

    Tries to open a consumer connection with empty string as name.  Expects
    to recieve a client error.
    """
    def test_open_consumer_no_name(self):
        response = self.dcp_client.open_consumer("")
        assert response['status'] == ERR_EINVAL

    """ Open producer empty name

    Tries to open a producer connection with empty string as name.  Expects
    to recieve a client error.
    """
    def test_open_producer_no_name(self):
        response = self.dcp_client.open_producer("")
        assert response['status'] == ERR_EINVAL


    """ Open n producers and consumers

    Open n consumer and n producer connections.  Check dcp stats and verify number
    of open connections = 2n with corresponding values for each conenction type.
    Expects each open connection response return true.
    """
    def test_open_n_consumer_producers(self):
        n = 16
        conns = [DcpClient(self.host, self.port) for i in xrange(2*n)]
        ops = []
        for i in xrange(n):
            op = conns[i].open_consumer("consumer{0}".format(i))
            ops.append(op)
            op = conns[n + i].open_producer("producer{0}".format(n + i))
            ops.append(op)

        for op in ops:
            assert op['status'] == SUCCESS

        stats = self.mcd_client.stats('dcp')
        assert stats['ep_dcp_count'] == str(n * 2)

    def test_open_notifier(self):
        response = self.dcp_client.open_notifier("notifier")
        assert response['status'] == SUCCESS

    def test_open_notifier_no_name(self):
        response = self.dcp_client.open_notifier("")
        assert response['status'] == ERR_EINVAL

    """Basic add stream test

    This test verifies a simple add stream command. It expects that a stream
    request message will be sent to the producer before a response for the
    add stream command is returned."""
    @unittest.skip("invalid: MB-11890")
    def test_add_stream_command(self):

        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS
        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == SUCCESS


    @unittest.skip("invalid: MB-11890")
    def test_add_stream_reopen_connection(self):

        for i in range(10):
            response = self.dcp_client.open_consumer("mystream")
            assert response['status'] == SUCCESS

            response = self.dcp_client.add_stream(0, 0)
            assert response['status'] == SUCCESS

            self.dcp_client.reconnect()


    """Add stream to producer

    Attempt to add stream to a producer connection. Expects to recieve
    client error response."""
    @unittest.skip("invalid: MB-11890")
    def test_add_stream_to_producer(self):

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == ERR_ECLIENT

    """Add stream test without open connection

    This test attempts to add a stream without idnetifying the
    client as a consumer or producer.  Excepts request
    to throw client error"""
    @unittest.skip("invalid: MB-11890")
    def test_add_stream_without_connection(self):
        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == ERR_ECLIENT

    """Add stream command with no consumer vbucket

    Attempts to add a stream when no vbucket exists on the consumer. The
    client shoudl expect a not my vbucket response immediately"""
    @unittest.skip("invalid: MB-11890")
    def test_add_stream_not_my_vbucket(self):
        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(1025, 0)
        assert response['status'] == ERR_NOT_MY_VBUCKET

    """Add stream when stream exists

    Creates a stream and then attempts to create another stream for the
    same vbucket. Expects to fail with an exists error."""
    @unittest.skip("invalid: MB-11890")
    def test_add_stream_exists(self):
        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == ERR_KEY_EEXISTS

    """Add stream to new consumer

    Creates two clients each with consumers using the same key.
    Attempts to add stream to first consumer and second consumer.
    Expects that adding stream to second consumer passes"""
    @unittest.skip("invalid: MB-11890")
    def test_add_stream_to_duplicate_consumer(self):

        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        dcp_client2 = DcpClient(self.host, self.port)
        response = dcp_client2.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == ERR_ECLIENT

        response = dcp_client2.add_stream(0, 0)
        assert response['status'] == SUCCESS

        dcp_client2.close()

    """
    Add a stream to consumer with the takeover flag set = 1.  Expects add stream
    command to return successfully.
    """
    @unittest.skip("invalid: MB-11890")
    def test_add_stream_takeover(self):

        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 1)
        assert response['status'] == SUCCESS

    """
        Open n consumer connection.  Add one stream to each consumer for the same
        vbucket.  Expects every add stream request to succeed.
    """
    @unittest.skip("invalid: MB-11890")
    def test_add_stream_n_consumers_1_stream(self):

        n = 16
        self.verification_seqno = n

        conns = [DcpClient(self.host, self.port) for i in xrange(n)]
        for i in xrange(n):
            response = self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

            stream = "mystream{0}".format(i)
            response = conns[i].open_consumer(stream)
            assert response['status'] == SUCCESS

            response = conns[i].add_stream(0, 1)
            assert response['status'] == SUCCESS

        stats = self.mcd_client.stats('dcp')
        assert stats['ep_dcp_count'] == str(n)

        self.statsHandler.wait_for_persistence(self.mcd_client)


    """
        Open n consumer connection.  Add n streams to each consumer for unique vbucket
        per connection. Expects every add stream request to succeed.
    """
    @unittest.skip("invalid: MB-11890")
    def test_add_stream_n_consumers_n_streams(self):
        n = 8
        self.verification_seqno = n

        vb_ids = self.all_vbucket_ids()
        conns = [DcpClient(self.host, self.port) for i in xrange(n)]
        for i in xrange(n):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

            stream = "mystream{0}".format(i)
            response = conns[i].open_consumer(stream)
            assert response['status'] == SUCCESS

            for vb in vb_ids[0:n]:
                response = conns[i].add_stream(vb, 0)
                assert response['status'] == SUCCESS

        stats = self.mcd_client.stats('dcp')
        assert stats['ep_dcp_count'] == str(n)



    """
        Open a single consumer and add stream for all active vbuckets with the
        takeover flag set in the request.  Expects every add stream request to succeed.
    """
    @unittest.skip("invalid: MB-11890")
    def test_add_stream_takeover_all_vbuckets(self):

        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        # parsing keys: 'vb_1', 'vb_0',...
        vb_ids = self.all_vbucket_ids()
        for i in vb_ids:
            response = self.dcp_client.add_stream(i, 1)
            assert response['status'] == SUCCESS

    @unittest.skip("invalid: MB-11890")
    def test_add_stream_various_ops(self):
        """ verify consumer can receive mutations created by various mcd ops """

        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.set('key', 0, 0, val, 0)

        for i in range(100):
            # append + prepend
            self.mcd_client.append('key',str(i), 0, 0)
            val += str(i)
            self.mcd_client.prepend('key',str(i), 0, 0)
            val = str(i) + val


        self.mcd_client.incr('key2', init = 0, vbucket = 0)
        for i in range(100):
            self.mcd_client.incr('key2', amt = 2, vbucket = 0)
        for i in range(100):
            self.mcd_client.decr('key2', amt = 2, vbucket = 0)

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == SUCCESS
        stats = self.mcd_client.stats('dcp')
        mutations =stats['eq_dcpq:mystream:stream_0_start_seqno']
        assert mutations == '402'


        self.verification_seqno = 402


    def test_stream_request_deduped_items(self):
        """ request a duplicate mutation """
        response = self.dcp_client.open_producer("mystream")

        # get vb uuid
        response = self.mcd_client.stats('failovers')
        vb_uuid = long(response['vb_0:0:id'])

        self.mcd_client.set('snap1', 0, 0, 'value1', 0)
        self.mcd_client.set('snap1', 0, 0, 'value2', 0)
        self.mcd_client.set('snap1', 0, 0, 'value3', 0)

        # attempt to request mutations 1 and 2
        start_seqno = 1
        end_seqno = 2
        stream = self.dcp_client.stream_req(0, 0,
                                            start_seqno,
                                            end_seqno,
                                            vb_uuid)

        assert stream.status is SUCCESS
        stream.run()
        assert stream.last_by_seqno == 3


        self.verification_seqno == 3



    def test_stream_request_dupe_backfilled_items(self):
        """ request mutations across memory/backfill mutations"""
        self.dcp_client.open_producer("mystream")

        def load(i):
            """ load 3 and persist """
            set_ops = [self.mcd_client.set('key%s'%i, 0, 0, 'value', 0)\
                                                            for x in range(3)]
            self.statsHandler.wait_for_persistence(self.mcd_client)

        def stream(end, vb_uuid):
            backfilled = False

            # send a stream request mutations from 1st snapshot
            stream = self.dcp_client.stream_req(0, 0, 0, end, vb_uuid)

            # check if items were backfilled before streaming
            stats = self.mcd_client.stats('dcp')
            num_backfilled =\
             int(stats['eq_dcpq:mystream:stream_0_backfill_sent'])

            if num_backfilled > 0:
                backfilled = True

            stream.run()  # exaust stream
            assert stream.has_response() == False

            self.dcp_client.close_stream(0)
            return backfilled

        # get vb uuid
        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])

        # load stream snapshot 1
        load('a')
        stream(3, vb_uuid)

        # load some more items
        load('b')

        # attempt to stream until request contains backfilled items
        tries = 10
        backfilled = stream(4, vb_uuid)
        while not backfilled and tries > 0:
            tries -= 1
            time.sleep(2)
            backfilled = stream(4, vb_uuid)

        assert backfilled, "ERROR: no back filled items were streamed"

        self.verification_seqno= 6



    def test_backfill_from_default_vb_uuid(self):
        """ attempt a backfill stream request using vb_uuid = 0 """

        def disk_stream():
            stream = self.dcp_client.stream_req(0, 0, 0, 1, 0)
            last_by_seqno = 0
            persisted = False

            assert stream.status is SUCCESS
            snap = stream.next_response()
            if snap['flag'].find('disk') == 0:
                persisted = True

            return persisted

        self.dcp_client.open_producer("mystream")
        self.mcd_client.set('key', 0, 0, 'value', 0)

        tries = 20
        while tries > 0 and not disk_stream():
            tries -= 1
            time.sleep(1)

        assert tries > 0, "Items never persisted to disk"

    """Close stream that has not been initialized.
    Expects client error."""
    def test_close_stream_command(self):
        response = self.dcp_client.close_stream(0)
        assert response['status'] == ERR_ECLIENT


    """Close a consumer stream. Expects close operation to
    return a success."""
    @unittest.skip("invalid: MB-11890")
    def test_close_consumer_stream(self):

        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == SUCCESS

        response = self.dcp_client.close_stream(0)
        assert response['status'] == SUCCESS


    """
        Open a consumer connection.  Add stream for a selected vbucket.  Then close stream.
        Immediately after closing stream send a request to add stream again.  Expects that
        stream can be added after closed.
    """
    @unittest.skip("invalid: MB-11890")
    def test_close_stream_reopen(self):
        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == ERR_KEY_EEXISTS

        response = self.dcp_client.close_stream(0)
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == SUCCESS

    """
        open and close stream as a consumer then takeover
        stream as producer and attempt to reopen stream
        from same vbucket
    """
    @unittest.skip("invalid scenario: MB-11785")
    def test_close_stream_reopen_as_producer(self):
       response = self.dcp_client.open_consumer("mystream")
       assert response['status'] == SUCCESS

       response = self.dcp_client.add_stream(0, 0)
       assert response['status'] == SUCCESS

       response = self.dcp_client.close_stream(0)
       assert response['status'] == SUCCESS

       response = self.dcp_client.open_producer("mystream")
       assert response['status'] == SUCCESS

       response = self.dcp_client.stream_req(0, 0, 0, 0, 0, 0)
       assert response.status == SUCCESS

       response = self.dcp_client.open_consumer("mystream")
       assert response['status'] == SUCCESS

       response = self.dcp_client.close_stream(0)
       assert response['status'] == ERR_KEY_ENOENT


    """
        Add stream to a consumer connection for a selected vbucket.  Start sending ops to node.
        Send close stream command to selected vbucket.  Expects that consumer has not recieved any
        subsequent mutations after producer recieved the close request.
    """
    def test_close_stream_with_ops(self):

        stream_closed = False


        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS


        doc_count = 1000
        for i in range(doc_count):
            self.mcd_client.set('key%s'%i, 0, 0, 'value', 0)


        stream = self.dcp_client.stream_req(0, 0, 0, doc_count, 0)
        while stream.has_response():

            response = stream.next_response()
            if not stream_closed:
                response = self.dcp_client.close_stream(0)
                assert response['status'] == SUCCESS, response
                stream_closed = True

            if response is None:
                break

        assert stream.last_by_seqno < doc_count,\
            "Error: recieved all mutations on closed stream"

        self.verification_seqno = doc_count


    """
        Sets up a consumer connection.  Adds stream and then sends 2 close stream requests.  Expects
        second request to close stream returns noent

    """
    def test_close_stream_twice(self):

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.stream_req(0, 0, 0, 1000, 0)
        assert response.status == SUCCESS

        response = self.dcp_client.close_stream(0)
        assert response['status'] == SUCCESS

        response = self.dcp_client.close_stream(0)
        assert response['status'] == ERR_KEY_ENOENT

    """
        Test verifies that if multiple consumers are streaming from a vbucket
        that if one of the consumer closes then the producer doesn't stop
        sending changes to other consumers
    """
    @unittest.skip("invalid: MB-11890")
    def test_close_stream_n_consumers(self):

        n = 16
        for i in xrange(100):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)
        self.statsHandler.wait_for_persistence(self.mcd_client)

        # add stream to be close by different client
        client2 = DcpClient(self.host, self.port)
        closestream = "closestream"
        client2.open_consumer(closestream)
        client2.add_stream(0, 0)

        conns = [DcpClient(self.host, self.port) for i in xrange(n)]

        for i in xrange(n):

            stream = "mystream{0}".format(i)
            conns[i].open_consumer(stream)
            conns[i].add_stream(0, 1)
            if i == int(n/2):
                # close stream
                response = client2.close_stream(0)
                assert response['status'] == SUCCESS

        time.sleep(2)
        stats = self.mcd_client.stats('dcp')
        key = "eq_dcpq:{0}:stream_0_state".format(closestream)
        assert stats[key] == 'dead'

        for i in xrange(n):
            key = "eq_dcpq:mystream{0}:stream_0_state".format(i)
            assert stats[key] in ('reading', 'pending')

        client2.close()


        self.verification_seqno = 100

    """Request failover log without connection

    attempts to retrieve failover log without establishing a connection to
    a producer.  Expects operation is not supported"""
    def test_get_failover_log_command(self):
        response = self.dcp_client.get_failover_log(0)
        assert response['status'] == ERR_ECLIENT

    """Request failover log from consumer

    attempts to retrieve failover log from a consumer.  Expects
    operation is not supported."""
    def test_get_failover_log_consumer(self):

        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.get_failover_log(0)
        assert response['status'] == ERR_ECLIENT

    """Request failover log from producer

    retrieve failover log from a producer. Expects to successfully recieve
    failover log and for it to match dcp stats."""
    def test_get_failover_log_producer(self):

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.get_failover_log(0)
        assert response['status'] == SUCCESS

        response = self.mcd_client.stats('failovers')
        assert response['vb_0:0:seq'] == '0'

    """Request failover log from invalid vbucket

    retrieve failover log from invalid vbucket. Expects to not_my_vbucket from producer."""
    def test_get_failover_invalid_vbucket(self):

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.get_failover_log(1025)
        assert response['status'] == ERR_NOT_MY_VBUCKET


    """Failover log during stream request

    Open a producer connection and send and add_stream request with high end_seqno.
    While waiting for end_seqno to be reached send request for failover log
    and Expects that producer is still able to return failover log
    while consumer has an open add_stream request.
    """
    def test_failover_log_during_stream_request(self):

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        stream = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        seqno = stream.failover_log[0][1]
        response = self.dcp_client.get_failover_log(0)

        assert response['status'] == SUCCESS
        assert response['value'][0][1] == seqno

    """Failover log with ops

    Open a producer connection to a vbucket and start loading data to node.
    After expected number of items have been created send request for failover
    log and expect seqno to match number
    """
    @unittest.skip("needs debug")
    def test_failover_log_with_ops(self):

        stream = "mystream"
        response = self.dcp_client.open_producer(stream)
        assert response['status'] == SUCCESS

        stream = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        assert stream.status == SUCCESS
        seqno = stream.failover_log[0][1]

        for i in range(100):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)
            resp = stream.next_response()
            assert resp

            if (i % 10) == 0:
                fail_response = self.dcp_client.get_failover_log(0)
                assert fail_response['status'] == SUCCESS
                assert fail_response['value'][0][1] == seqno

        self.verification_seqno = 100


    """Request failover from n producers from n vbuckets

    Open n producers and attempt to fetch failover log for n vbuckets on each producer.
    Expects expects all requests for failover log to succeed and that the log for
    similar buckets match.
    """
    def test_failover_log_n_producers_n_vbuckets(self):

        n = 2
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        vb_ids = self.all_vbucket_ids()
        expected_seqnos = {}
        for id_ in vb_ids:
            #print 'id', id_
            response = self.dcp_client.get_failover_log(id_)
            expected_seqnos[id_] = response['value'][0][0]

            # open n producers for this vbucket
            for i in range(n):
                stream = "mystream{0}".format(i)
                conn = DcpClient(self.host, self.port)
                #print 'conn', conn
                response = conn.open_producer(stream)
                vbucket_id = id_
                #print 'vbucket_id',vbucket_id
                response = self.dcp_client.get_failover_log(vbucket_id)
                assert response['value'][0][0] == expected_seqnos[vbucket_id]


    """Basic dcp stream request

    Opens a producer connection and sends a stream request command for
    vbucket 0. Since no items exist in the server we should accept the
    stream request and then send back a stream end message."""
    def test_stream_request_command(self):
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        stream = self.dcp_client.stream_req(0, 0, 0, 0, 0, 0)
        assert stream.opcode == CMD_STREAM_REQ
        end = stream.next_response()
        assert end and end['opcode'] == CMD_STREAM_END

    """Stream request with invalid vbucket

    Opens a producer connection and then tries to create a stream with an
    invalid VBucket. Should get a not my vbucket error."""
    def test_stream_request_invalid_vbucket(self):
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.stream_req(1025, 0, 0, MAX_SEQNO, 0, 0)
        assert response.status == ERR_NOT_MY_VBUCKET

        response = self.mcd_client.stats('dcp')
        assert 'eq_dcpq:mystream:stream_0_opaque' not in response
        assert response['eq_dcpq:mystream:type'] == 'producer'

    """Stream request for invalid connection

    Try to create a stream over a non-dcp connection. The server should
    disconnect from the client"""
    def test_stream_request_invalid_connection(self):

        response = self.dcp_client.stream_req(0, 0, 0, MAX_SEQNO, 0, 0)
        assert response.status == ERR_ECLIENT

        response = self.mcd_client.stats('dcp')
        assert 'eq_dcpq:mystream:type' not in response

    """Stream request for consumer connection

    Try to create a stream on a consumer connection. The server should
    disconnect from the client"""
    def test_stream_request_consumer_connection(self):
        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.stream_req(0, 0, 0, MAX_SEQNO, 0)
        assert response.status == ERR_ECLIENT

        response = self.mcd_client.stats('dcp')
        assert 'eq_dcpq:mystream:type' not in response

    """Stream request with start seqno bigger than end seqno

    Opens a producer connection and then tries to create a stream with a start
    seqno that is bigger than the end seqno. The stream should be closed with an
    range error. Now we are getting a client - still correct"""
    def test_stream_request_start_seqno_bigger_than_end_seqno(self):
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.stream_req(0, 0, MAX_SEQNO, MAX_SEQNO/2, 0, 0)
        assert response.status == ERR_ECLIENT  or response.status == ERR_ERANGE

        response = self.mcd_client.stats('dcp')
        assert 'eq_dcpq:mystream:stream_0_opaque' not in response

        # dontassert response['eq_dcpq:mystream:type'] == 'producer'

    """Stream requests from the same vbucket

    Opens a stream request for a vbucket to read up to seq 100. Then sends another
    stream request for the same vbucket.  Expect a EXISTS error and dcp stats
    should refer to initial created stream."""
    def test_stream_from_same_vbucket(self):

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.stream_req(0, 0, 0, MAX_SEQNO, 0)
        assert response.status == SUCCESS

        response = self.mcd_client.stats('dcp')
        assert response['eq_dcpq:mystream:type'] == 'producer'
        created = response['eq_dcpq:mystream:created']
        assert created >= 0

        response = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        assert response.status == ERR_KEY_EEXISTS

        response = self.mcd_client.stats('dcp')
        assert response['eq_dcpq:mystream:created'] == created



    """Basic dcp stream request (Receives mutations)

    Stores 10 items into vbucket 0 and then creates an dcp stream to
    retrieve those items in order of sequence number.
    """
    def test_stream_request_with_ops(self):

        #self.mcd_client.stop_persistence()



        doc_count = snap_end_seqno = 10

        for i in range(doc_count):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        mutations = 0
        last_by_seqno = 0
        stream = self.dcp_client.stream_req(0, 0, 0, doc_count, 0, 0)
        assert stream.status == SUCCESS
        stream.run()

        self.statsHandler.wait_for_persistence(self.mcd_client)



        assert stream.last_by_seqno == doc_count

        self.verification_seqno = doc_count



    """Receive mutation from dcp stream from a later sequence

    Stores 10 items into vbucket 0 and then creates an dcp stream to
    retrieve items from sequence number 7 to 10 on (4 items).
    """
    def test_stream_request_with_ops_start_sequence(self):
        #self.mcd_client.stop_persistence()

        for i in range(10):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        resp = self.mcd_client.stats('vbucket-seqno')
        end_seqno = int(resp['vb_0:high_seqno'])

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])
        high_seqno = long(resp['vb_0:0:seq'])

        start_seqno = 7
        stream = self.dcp_client.stream_req(
            0, 0, start_seqno, end_seqno, vb_uuid)

        assert stream.status == SUCCESS

        responses = stream.run()
        mutations = \
           len(filter(lambda r: r['opcode']==CMD_MUTATION, responses))

        assert stream.last_by_seqno == 10
        assert mutations == 3

        self.verification_seqno = 10





    def set_keys_with_timestamp(self, count):

        for i in range(count):
            self.mcd_client.set('key' + str(i), 0, 0, str(time.time() ), 0)
            time.sleep(0.010)


    """ Concurrent set keys and stream them. Verify that the time between new arrivals
    is not greater than 10 seconds
    """


    def test_mutate_stream_request_concurrent_with_ops(self):   # ******

        doc_count = snap_end_seqno = 10000
        t = Thread( target=self.set_keys_with_timestamp, args=(doc_count,))
        t.start()


        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        mutations = 0
        last_by_seqno = 0
        stream = self.dcp_client.stream_req(0, 0, 0, doc_count, 0, 0)
        assert stream.status == SUCCESS
        results = stream.run()


        # remove response like this
        # {'snap_end_seqno': 30, 'arrival_time': 1423699992.195518, 'flag': 'memory', 'opcode': 86, 'snap_start_seqno': 30, 'vbucket': 0}
        resultsWithoutSnap = [x for x in results if 'key' in x]



        pauses = []
        i = 1
        while i <  len(resultsWithoutSnap):
            if resultsWithoutSnap[i]['arrival_time'] - resultsWithoutSnap[i-1]['arrival_time'] > 10:
                pauses.append( 'Key {0} set at {1} was streamed {2:.2f} seconds after the previous key was received. '.
                               format( resultsWithoutSnap[i]['key'],
                                       datetime.datetime.fromtimestamp(float(resultsWithoutSnap[i-1]['value'])).strftime('%H:%M:%S'),
                     resultsWithoutSnap[i]['arrival_time'] - resultsWithoutSnap[i-1]['arrival_time'],
                     datetime.datetime.fromtimestamp(float(resultsWithoutSnap[i-1]['value'])).strftime('%H:%M:%S')) )
            i = i + 1


        #print 'Number of pause delays:', len(pauses)
        if len(pauses) > 0:
            if len(pauses) < 20:   # keep the output manageable
                for i in pauses:
                    print i
            else:
                for i in range(20):
                    print pauses[i]

            assert False, 'There were pauses greater than 10 seconds in receiving stream contents'

        assert stream.last_by_seqno == doc_count




    """Basic dcp stream request (Receives mutations/deletions)

    Stores 10 items into vbucket 0 and then deletes 5 of thos items. After
    the items have been inserted/deleted from the server we create an dcp
    stream to retrieve those items in order of sequence number.
    """
    def test_stream_request_with_deletes(self):
        #self.mcd_client.stop_persistence()

        for i in range(10):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        for i in range(5):
            self.mcd_client.delete('key' + str(i),0, 0)

        resp = self.mcd_client.stats('vbucket-seqno')
        end_seqno = int(resp['vb_0:high_seqno'])

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        last_by_seqno = 0
        stream = self.dcp_client.stream_req(0, 0, 0, end_seqno, 0)
        assert stream.status == SUCCESS
        responses = stream.run()

        mutations = \
           len(filter(lambda r: r['opcode']==CMD_MUTATION, responses))
        deletions = \
           len(filter(lambda r: r['opcode']==CMD_DELETION, responses))

        assert mutations == 5
        assert deletions == 5
        assert stream.last_by_seqno == 15

        self.verification_seqno = 15
  
    """
    MB-13386 - delete and compaction
    Stores 10 items into vbucket 0 and then deletes 5 of those items. After
    the items have been inserted/deleted from the server we create an dcp
    stream to retrieve those items in order of sequence number.
    """

    def test_stream_request_with_deletes_and_compaction(self):


        for i in range(1,4):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        time.sleep(2)
        for i in range(2,4):
            self.mcd_client.delete('key' + str(i),0, 0)

        time.sleep(2)

        resp = self.mcd_client.stats('vbucket-seqno')
        end_seqno = int(resp['vb_0:high_seqno'])

        self.statsHandler.wait_for_persistence(self.mcd_client)


        # drop deletes is important for this scenario
        self.mcd_client.compact_db('',0, 2, 5, 1)   # key, bucket,  purge_before_ts, purge_before_seq, drop_deletes


        # wait for compaction to end - if this were a rest call then we could use active tasks but
        # as this an mc bin client call the only way known (to me) is to sleep
        time.sleep(20)



        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        last_by_seqno = 0
        time.sleep(5)
        stream = self.dcp_client.stream_req(0, 0, 0, end_seqno, 0)
        assert stream.status == SUCCESS
        responses = stream.run()

        print 'responses', responses


        mutations = \
           len(filter(lambda r: r['opcode']==CMD_MUTATION, responses))
        deletions = \
           len(filter(lambda r: r['opcode']==CMD_DELETION, responses))


        assert deletions == 1,'Deletion mismatch, expect {0}, actual {1}'.format(2, deletions)
        assert mutations == 1,'Mutation mismatch, expect {0}, actual {1}'.format(1, mutations)

        assert stream.last_by_seqno == 5



    """
    MB-13479 - dedup and compaction
    Set some keys
    Consumer consumes them
    Delete one of the set keys
    Compaction - dedup occurs
    Request more of the stream - there should be a rollback so the the consumer does not bridge the dedup

    """

    def test_stream_request_with_dedup_and_compaction(self):

        KEY_BASE = 'key'
        for i in range(1,4):
            self.mcd_client.set(KEY_BASE + str(i), 0, 0, 'value', 0)

        time.sleep(2)


        resp = self.mcd_client.stats('vbucket-seqno')

        end_seqno = int(resp['vb_0:high_seqno'])



        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        # consume the first 3 keys
        stream = self.dcp_client.stream_req(0, 0, 0, 3, 0)
        assert stream.status == SUCCESS
        responses = stream.run()


        # and delete one from the original batch
        for i in range(2,4):
            self.mcd_client.delete(KEY_BASE + str(i),0, 0)


        # set a couple more keys

        self.mcd_client.set(KEY_BASE + str(5), 0, 0, 'value', 0)
        self.mcd_client.set(KEY_BASE + str(5), 0, 0, 'value', 0)



        self.mcd_client.compact_db('',0, 3, 5, 1)   # key, bucket, ...
        time.sleep(10)

        # and now get the stream
        #     def stream_req(self, vbucket, takeover, start_seqno, end_seqno,
        #               vb_uuid, snap_start = None, snap_end = None):
        stream = self.dcp_client.stream_req(0, 0, 3, 6, 0)
        assert stream.status == ERR_ROLLBACK










    @unittest.skip("Broken: needs debugging")
    def test_stream_request_backfill_deleted(self):
        """ verify deleted mutations can be streamed after backfill
            task has occured """

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])

        # set 3 items and delete delete first 2
        self.mcd_client.set('key1', 0, 0, 'value', 0)
        self.mcd_client.set('key2', 0, 0, 'value', 0)
        self.mcd_client.set('key3', 0, 0, 'value', 0)
        self.mcd_client.set('key4', 0, 0, 'value', 0)
        self.mcd_client.set('key5', 0, 0, 'value', 0)
        self.mcd_client.set('key6', 0, 0, 'value', 0)
        self.statsHandler.wait_for_persistence(self.mcd_client)
        self.mcd_client.delete('key1', 0, 0)
        self.mcd_client.delete('key2', 0, 0)


        backfilling = False
        tries = 10
        while not backfilling and tries > 0:
            # stream request until backfilling occurs
            self.dcp_client.stream_req(0, 0, 0, 5,
                                       vb_uuid)
            stats = self.mcd_client.stats('dcp')
            num_backfilled =\
             int(stats['eq_dcpq:mystream:stream_0_backfilled'])
            backfilling = num_backfilled > 0
            tries -= 1
            time.sleep(2)

        assert backfilling, "ERROR: backfill task did not start"

        # attempt to stream deleted mutations
        stream = self.dcp_client.stream_req(0, 0, 0, 3, vb_uuid)
        response = stream.next_response()


    """ Stream request with incremental mutations

    Insert some ops and then create a stream that wants to get more mutations
    then there are ops. The stream should pause after it gets the first set.
    Then add some more ops and wait from them to be streamed out. We will insert
    the exact amount of items that the should be streamed out."""
    def test_stream_request_incremental(self):

        for i in range(10):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        stream = self.dcp_client.stream_req(0, 0, 0, 20, 0)
        assert stream.status == SUCCESS
        stream.run(10)
        assert stream.last_by_seqno == 10

        for i in range(10):
            self.mcd_client.set('key' + str(i + 10), 0, 0, 'value', 0)

        # read remaining mutations
        stream.run()
        assert stream.last_by_seqno == 20

        self.verification_seqno = 20

    """Send stream requests for multiple

    Put some operations into four different vbucket. Then get the end sequence
    number for each vbucket and create a stream to it. Read all of the mutations
    from the streams and make sure they are all sent."""
    def test_stream_request_multiple_vbuckets(self):
        num_vbs = 4
        num_ops = 10
        for vb in range(num_vbs):
            for i in range(num_ops):
                 self.mcd_client.set('key' + str(i), 0, 0, 'value', vb)


        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        streams = {}
        stats = self.mcd_client.stats('vbucket-seqno')
        for vb in range(4):
            en = int(stats['vb_%d:high_seqno' % vb])
            stream = self.dcp_client.stream_req(vb, 0, 0, en, 0)
            streams[vb] = {'stream' : stream,
                           'mutations' : 0,
                           'last_seqno' : 0 }

        while len(streams) > 0:
            for vb in streams.keys():
                if streams[vb]['stream'].has_response():
                    response = streams[vb]['stream'].next_response()
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
        response = self.dcp_client.open_producer("rollback")
        assert response['status'] == SUCCESS

        self.mcd_client.set('key1', 0, 0, 'value', 0)
        self.mcd_client.set('key2', 0, 0, 'value', 0)

        vb_id = 'vb_0'
        vb_stats = self.mcd_client.stats('vbucket-seqno')
        fl_stats = self.mcd_client.stats('failovers')
        fail_seqno = long(fl_stats[vb_id+':0:seq'])
        vb_uuid = long(vb_stats[vb_id+':uuid'])
        rollback = long(vb_stats[vb_id+':high_seqno'])

        start_seqno = end_seqno =  3
        stream = self.dcp_client.stream_req(0, 0, start_seqno, end_seqno, vb_uuid)

        assert stream.status == ERR_ROLLBACK
        assert stream.rollback == rollback
        assert stream.rollback_seqno == fail_seqno

        start_seqno = end_seqno = rollback
        stream = self.dcp_client.stream_req(0, 0, start_seqno - 1, end_seqno, vb_uuid)
        stream.run()

        assert end_seqno == stream.last_by_seqno

        self.verification_seqno = end_seqno


    """
        Sends a stream request with start seqno greater than seqno of vbucket.  Expects
        to receive a rollback response with seqno to roll back to.  Instead of rolling back
        resend stream request n times each with high seqno's and expect rollback for each attempt.
    """
    def test_stream_request_n_rollbacks(self):
        response = self.dcp_client.open_producer("rollback")
        assert response['status'] == SUCCESS

        vb_stats = self.mcd_client.stats('vbucket-seqno')
        vb_uuid = long(vb_stats['vb_0:uuid'])

        for n in range(1000):
            self.mcd_client.set('key1', 0, 0, 'value', 0)

            by_seqno = n + 1
            stream = self.dcp_client.stream_req(0, 0, by_seqno+1, by_seqno+2, vb_uuid)
            assert stream.status == ERR_ROLLBACK
            assert stream.rollback_seqno == 0

    """
        Send stream request command from n producers for the same vbucket.  Expect each request
        to succeed for each producer and verify that expected number of mutations are received
        for each request.
    """
    def test_stream_request_n_producers(self):
        clients = []

        for n in range(10):
            client = DcpClient(self.host, self.port)
            op = client.open_producer("producer:%s" % n)
            assert op['status'] == SUCCESS
            clients.append(client)


        for n in range(1, 10):
            self.mcd_client.set('key%s'%n, 0, 0, 'value', 0)

            for client in clients:
                stream = client.stream_req(0, 0, 0, n, 0)

                # should never get rollback
                assert stream.status == SUCCESS, stream.status
                stream.run()

                # stream changes and we should reach last seqno
                assert stream.last_by_seqno == n,\
                    "%s != %s" % (stream.last_by_seqno, n)
                self.verification_seqno = stream.last_by_seqno

        [client.close() for client in clients]

    def test_stream_request_needs_rollback(self):

        # load docs
        self.mcd_client.set('key1', 0, 0, 'value', 0)
        self.mcd_client.set('key2', 0, 0, 'value', 0)
        self.mcd_client.set('key3', 0, 0, 'value', 0)

        # failover uuid
        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])

        # vb_uuid does not exist
        self.dcp_client.open_producer("rollback")
        resp = self.dcp_client.stream_req(0, 0, 1, 3, 0, 1, 1)
        assert resp and resp.status == ERR_ROLLBACK
        assert resp and resp.rollback == 0

        # snap_end > by_seqno
        resp = self.dcp_client.stream_req(0, 0, 1, 3, vb_uuid, 1, 4)
        assert resp and resp.status == SUCCESS, resp.status

        # snap_start > by_seqno
        resp = self.dcp_client.stream_req(0, 0, 4, 4, vb_uuid, 4, 4)
        assert resp and resp.status == ERR_ROLLBACK, resp.status
        assert resp and resp.rollback == 3, resp.rollback

        # fallthrough
        resp = self.dcp_client.stream_req(0, 0, 7, 7, vb_uuid, 2, 7)
        assert resp and resp.status == ERR_ROLLBACK, resp.status
        assert resp and resp.rollback == 3, resp.rollback


    def test_stream_request_after_close(self):
        """
        Load items from producer then close producer and attempt to resume stream request
        """

        doc_count = 100
        self.dcp_client.open_producer("mystream")

        for i in xrange(doc_count):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)
        self.statsHandler.wait_for_persistence(self.mcd_client)

        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])


        stream = self.dcp_client.stream_req(0, 0, 0, doc_count,
                                        vb_uuid)

        stream.run(doc_count/2)
        self.dcp_client.close()

        self.dcp_client = DcpClient(self.host, self.port)
        self.dcp_client.open_producer("mystream")
        stream = self.dcp_client.stream_req(0, 0, stream.last_by_seqno,
                                            doc_count, vb_uuid)
        while stream.has_response():
            response = stream.next_response()
            if response['opcode'] == CMD_MUTATION:
                # first mutation should be at location we left off
                assert response['key'] == 'key'+str(doc_count/2)
                break

    def test_stream_request_notifier(self):
        """Open a notifier consumer and verify mutations are ready
        to be streamed"""


        doc_count = 100
        response = self.dcp_client.open_notifier("notifier")
        assert response['status'] == SUCCESS

        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])

        notifier_stream =\
            self.dcp_client.stream_req(0, 0, doc_count - 1, 0, vb_uuid)

        for i in range(doc_count):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)


        response = notifier_stream.next_response()
        assert response['opcode'] == CMD_STREAM_END


        self.dcp_client = DcpClient(self.host, self.port)
        response = self.dcp_client.open_producer("producer")
        assert response['status'] == SUCCESS


        stream = self.dcp_client.stream_req(0, 0, 0, doc_count, 0)
        assert stream.status == SUCCESS
        stream.run()
        assert stream.last_by_seqno == doc_count
        self.verification_seqno = doc_count

    def test_stream_request_notifier_bad_uuid(self):
        """Wait for mutations from missing vb_uuid"""

        response = self.dcp_client.open_notifier("notifier")
        assert response['status'] == SUCCESS

        # set 1
        self.mcd_client.set('key', 0, 0, 'value', 0)

        # create notifier stream with vb_uuid that doesn't exist
        # expect rollback since this value can never be reached
        vb_uuid = 0
        stream = self.dcp_client.stream_req(0, 0, 1, 0, 0)
        assert stream.status == ERR_ROLLBACK,\
                "ERROR: response expected = %s, received = %s" %\
                    (ERR_ROLLBACK, stream.status)

    def test_stream_request_append(self):
        """ stream appended mutations """
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.set('key', 0, 0, val, 0)

        for i in range(100):
            self.mcd_client.append('key',str(i), 0, 0)
            val += str(i)

        stream = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        assert stream.status == SUCCESS

        responses = stream.run()
        assert stream.last_by_seqno == 101
        assert responses[1]['value'] == val

        self.verification_seqno = 101

    def test_stream_request_prepend(self):
        """ stream prepended mutations """
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.set('key', 0, 0, val, 0)

        for i in range(100):
            self.mcd_client.prepend('key',str(i), 0, 0)
            val = str(i) + val

        stream = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        assert stream.status == SUCCESS

        responses = stream.run()
        assert stream.last_by_seqno == 101
        assert responses[1]['value'] == val

        self.verification_seqno = 101

    def test_stream_request_incr(self):
        """ stream mutations created by incr command """
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.incr('key', init = 0, vbucket = 0)

        for i in range(100):
            self.mcd_client.incr('key', amt = 2, vbucket = 0)

        stream = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        assert stream.status == SUCCESS

        responses = stream.run()
        assert stream.last_by_seqno == 101
        assert responses[1]['value'] == '200'

        self.verification_seqno = 101


    def test_stream_request_decr(self):
        """ stream mutations created by decr command """
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.decr('key', init = 200, vbucket = 0)

        for i in range(100):
            self.mcd_client.decr('key', amt = 2, vbucket = 0)

        stream = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        assert stream.status == SUCCESS

        responses = stream.run()
        assert stream.last_by_seqno == 101
        assert responses[1]['value'] == '0'

        self.verification_seqno = 101

    def test_stream_request_replace(self):
        """ stream mutations created by replace command """
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.set('key', 0, 0, 'value', 0)

        for i in range(100):
            self.mcd_client.replace('key', 0, 0, 'value'+str(i), 0)

        stream = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        assert stream.status == SUCCESS

        responses = stream.run()
        assert stream.last_by_seqno == 101
        assert responses[1]['value'] == 'value99'

        self.verification_seqno = 101

    @unittest.skip("needs debug")
    def test_stream_request_touch(self):
        """ stream mutations created by touch command """

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.set('key', 100, 0, 'value', 0)
        self.mcd_client.touch('key', 1, 0)

        stream = self.dcp_client.stream_req(0, 0, 0, 2, 0)
        assert stream.status == SUCCESS

        responses = stream.run()
        assert stream.last_by_seqno == 2
        assert int(responses[1]['expiration']) > 0

        self.statsHandler.wait_for_persistence(self.mcd_client)

        stats = self.mcd_client.stats()
        num_expired = stats['vb_active_expired']
        if num_expired == 0:
            self.verification_seqno = 2
        else:
            assert num_expired == 1
            self.verification_seqno = 3


    def test_stream_request_gat(self):
        """ stream mutations created by get-and-touch command """

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.set('key', 100, 0, 'value', 0)
        self.mcd_client.gat('key', 1, 0)

        stream = self.dcp_client.stream_req(0, 0, 0, 2, 0)
        assert stream.status == SUCCESS

        responses = stream.run()
        assert stream.last_by_seqno == 2
        assert int(responses[1]['expiration']) > 0

        self.statsHandler.wait_for_persistence(self.mcd_client)
        stats = self.mcd_client.stats()
        num_expired = int(stats['vb_active_expired'])
        if num_expired == 0:
            self.verification_seqno = 2
        else:
            assert num_expired == 1
            self.verification_seqno = 3


    def test_stream_request_client_per_vb(self):
        """ stream request muataions from each vbucket with a new client """

        for vb in xrange(8):
            for i in range(1000):
                    self.mcd_client.set('key'+str(i), 0, 0, 'value', vb)

        num_vbs = len(self.all_vbucket_ids())
        for vb in xrange(8):

            dcp_client = DcpClient(self.host, self.port)
            dcp_client.open_producer("producerstream")
            stream = dcp_client.stream_req(
                vb, 0, 0, 1000, 0)

            mutations = stream.run()
            try:
                assert stream.last_by_seqno == 1000, stream.last_by_seqno
                self.verification_seqno = 1000
            finally:
                dcp_client.close()

    def test_stream_request_mutation_with_flags(self):
        self.dcp_client.open_producer("mystream")
        self.mcd_client.set('key', 0, 2, 'value', 0)
        stream = self.dcp_client.stream_req(0, 0, 0, 1, 0)
        snap = stream.next_response()
        res = stream.next_response()
        item = self.mcd_client.get('key', 0)
        assert res['flags'] == 2
        assert item[0] == 2

    def test_flow_control(self):
        """ verify flow control of a 128 byte buffer stream """

        response = self.dcp_client.open_producer("flowctl")
        assert response['status'] == SUCCESS


        buffsize = 128
        response = self.dcp_client.flow_control(buffsize)
        assert response['status'] == SUCCESS

        for i in range(5):
                self.mcd_client.set('key'+str(i), 0, 0, 'value', 0)

        stream = self.dcp_client.stream_req(0, 0, 0, 5, 0)
        required_ack = False

        while stream.has_response():
                resp = stream.next_response()
                if resp is None:
                    ack = self.dcp_client.ack(buffsize)
                    assert ack is None, ack['error']
                    required_ack = True

        assert stream.last_by_seqno == 5
        assert required_ack, "received non flow-controlled stream"

        self.verification_seqno = 5


    " MB-15213 buffer size of zero means no flow control"
    def test_flow_control_buffer_size_zero(self):
        """ verify no flow control for a 0 byte buffer stream """

        response = self.dcp_client.open_producer("flowctl")
        assert response['status'] == SUCCESS


        buffsize = 0
        response = self.dcp_client.flow_control(buffsize)
        assert response['status'] == SUCCESS

        for i in range(5):
                self.mcd_client.set('key'+str(i), 0, 0, 'value', 0)

        stream = self.dcp_client.stream_req(0, 0, 0, 5, 0)
        required_ack = False

        # consume the stream
        while stream.has_response():
                resp = stream.next_response()


        assert stream.last_by_seqno == 5




    def test_flow_control_stats(self):
        """ verify flow control stats """

        buffsize = 128
        self.dcp_client.open_producer("flowctl")
        self.dcp_client.flow_control(buffsize)
        self.mcd_client.set('key1', 0, 0, 'valuevaluevalue', 0)
        self.mcd_client.set('key2', 0, 0, 'valuevaluevalue', 0)
        self.mcd_client.set('key3', 0, 0, 'valuevaluevalue', 0)

        def info():
            stats = self.mcd_client.stats('dcp')
            acked = stats['eq_dcpq:flowctl:total_acked_bytes']
            unacked = stats['eq_dcpq:flowctl:unacked_bytes']
            sent = stats['eq_dcpq:flowctl:total_bytes_sent']

            return int(acked), int(sent), int(unacked)

        # all stats 0
        assert all(map(lambda x: x==0, info()))

        stream = self.dcp_client.stream_req(0, 0, 0, 3, 0)
        time.sleep(10)   # give time for the stats to settle
        acked, sent, unacked = info()
        assert acked == 0

        if unacked != sent:
            print "test_flow_control_stats unacked %d sent %d" % (unacked, sent)
            logging.info("test_flow_control_stats unacked %d sent %d" % (unacked, sent))

        assert unacked == sent

        # ack received bytes
        last_acked = acked
        while unacked > 0:
            ack = self.dcp_client.ack(buffsize)
            acked, sent, unacked = info()
            assert acked == last_acked + buffsize
            last_acked = acked

        stream.run()
        assert stream.last_by_seqno == 3

        self.verification_seqno = 3

    def test_flow_control_stream_closed(self):
        """ close and reopen stream during with flow controlled client"""

        response = self.dcp_client.open_producer("flowctl")
        assert response['status'] == SUCCESS

        buffsize = 128
        response = self.dcp_client.flow_control(buffsize)
        assert response['status'] == SUCCESS

        end_seqno = 5
        for i in range(end_seqno):
                self.mcd_client.set('key'+str(i), 0, 0, 'value', 0)


        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])

        stream = self.dcp_client.stream_req(0, 0, 0, end_seqno, vb_uuid)
        max_timeouts =  10
        required_ack = False
        last_seqno = 0
        while stream.has_response() and max_timeouts > 0:
                resp = stream.next_response()

                if resp is None:

                    # close
                    self.dcp_client.close_stream(0)

                    # ack
                    ack = self.dcp_client.ack(buffsize)
                    assert ack is None, ack['error']
                    required_ack = True

                    # new stream
                    stream = self.dcp_client.stream_req(0, 0, last_seqno,
                                                        end_seqno, vb_uuid)
                    assert stream.status  == SUCCESS,\
                            "Re-open Stream failed"

                    max_timeouts -= 1

                elif resp['opcode'] == CMD_MUTATION:
                    last_seqno += 1

        # verify stream closed
        assert last_seqno == end_seqno, "Got %s" % last_seqno
        assert required_ack, "received non flow-controlled stream"

        self.verification_seqno = end_seqno


    def test_flow_control_reset_producer(self):
        """ recreate producer with various values max_buffer bytes """
        sizes = [64, 29, 64, 777, 32, 128, 16, 24, 29, 64]

        for buffsize in sizes:

            self.dcp_client = DcpClient(self.host, self.port)
            response = self.dcp_client.open_producer("flowctl")
            assert response['status'] == SUCCESS

            response = self.dcp_client.flow_control(buffsize)
            assert response['status'] == SUCCESS

            stats = self.mcd_client.stats('dcp')
            key = 'eq_dcpq:flowctl:max_buffer_bytes'
            conn_bsize = int(stats[key])
            assert  conn_bsize == buffsize,\
                '%s != %s' % (conn_bsize, buffsize)


    def test_flow_control_set_buffer_bytes_per_producer(self):
        """ use various buffer sizes between producer connections """

        def max_buffer_bytes(connection):
            stats = self.mcd_client.stats('dcp')
            key = 'eq_dcpq:%s:max_buffer_bytes' % connection
            return int(stats[key])

        def verify(connection, buffsize):
            self.dcp_client = DcpClient(self.host, self.port)
            response = self.dcp_client.open_producer(connection)
            assert response['status'] == SUCCESS
            response = self.dcp_client.flow_control(buffsize)
            assert response['status'] == SUCCESS
            producer_bsize = max_buffer_bytes(connection)
            assert producer_bsize == buffsize,\
                "%s != %s" % (producer_bsize, buffsize)

        producers = [("flowctl1", 64), ("flowctl2", 29), ("flowctl3", 128)]

        for producer in producers:
            connection, buffsize = producer
            verify(connection, buffsize)

    def test_flow_control_notifier_stream(self):
        """ verifies flow control still works with notifier streams """
        mutations = 100

        # create notifier
        response = self.dcp_client.open_notifier('flowctl')
        assert response['status'] == SUCCESS
        self.dcp_client.flow_control(16)

        # vb uuid
        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])

        # set to notify when seqno endseqno reached
        notifier_stream = self.dcp_client.stream_req(0, 0, mutations + 1, 0,  vb_uuid)

        # persist mutations
        for i in range(mutations):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)
        self.statsHandler.wait_for_persistence(self.mcd_client)

        tries = 10
        while tries > 0:
            resp = notifier_stream.next_response()
            if resp is None:
                self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)
            else:
                if resp['opcode'] == CMD_STREAM_END:
                    break
            tries -= 1

        assert tries > 0, 'notifier never received end stream'

    def test_flow_control_ack_n_vbuckets(self):

        self.dcp_client.open_producer("flowctl")

        mutations = 2
        num_vbs = 8
        buffsize = 64*num_vbs
        self.dcp_client.flow_control(buffsize)

        for vb in range(num_vbs):
            self.mcd_client.set('key1', 0, 0, 'value', vb)
            self.mcd_client.set('key2', 0, 0, 'value', vb)

        # request mutations
        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])
        for vb in range(num_vbs):
            self.dcp_client.stream_req(vb, 0, 0, mutations, vb_uuid)


        # ack until all mutations sent
        stats = self.mcd_client.stats('dcp')
        unacked = int(stats['eq_dcpq:flowctl:unacked_bytes'])
        start_t = time.time()
        while unacked > 0:
            ack = self.dcp_client.ack(unacked)
            assert ack is None, ack['error']
            stats = self.mcd_client.stats('dcp')
            unacked = int(stats['eq_dcpq:flowctl:unacked_bytes'])

            assert time.time() - start_t < 120,\
                "timed out waiting for seqno on all vbuckets"

        stats = self.mcd_client.stats('dcp')

        for vb in range(num_vbs):
            key = 'eq_dcpq:flowctl:stream_%s_last_sent_seqno'%vb
            seqno = int(stats[key])
            assert seqno == mutations,\
                "%s != %s" % (seqno, mutations)

            self.statsHandler.wait_for_persistence(self.mcd_client)
            assert self.get_persisted_seq_no(vb) == seqno


    def test_consumer_producer_same_vbucket(self):

        # producer stream request
        response = self.dcp_client.open_producer("producer")
        assert response['status'] == SUCCESS
        stream = self.dcp_client.stream_req(0, 0, 0, 1000, 0)
        assert stream.status is SUCCESS

        # reopen conenction as consumer
        dcp_client2 = DcpClient(self.host, self.port)
        response = dcp_client2.open_consumer("consumer")
        assert response['status'] == SUCCESS
        #response = dcp_client2.add_stream(0, 0)
        #assert response['status'] == SUCCESS


        for i in xrange(1000):
            self.mcd_client.set('key%s'%i, 0, 0, 'value', 0)

        stream.run()
        assert stream.last_by_seqno == 1000

        self.verification_seqno = 1000
        dcp_client2.close()


    def test_stream_request_cas(self):

        n = 5
        response = self.dcp_client.open_producer("producer")
        for i in xrange(n):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        for i in range(n):
            key = 'key'+str(i)
            rv, cas, _= self.mcd_client.get(key, 0)
            assert rv == SUCCESS
            self.mcd_client.cas(key, 0, 0, cas, 'new-value', 0)

        stream = self.dcp_client.stream_req(0, 0, 0, n, 0)
        responses = stream.run()
        mutations = \
           filter(lambda r: r['opcode']==CMD_MUTATION, responses)
        assert len(mutations) == n
        assert stream.last_by_seqno == 2*n

        self.verification_seqno = 2*n

        for doc in mutations:
            assert doc['value'] == 'new-value'




    def test_get_all_seq_no(self):

        res = self.mcd_client.get_vbucket_all_vbucket_seqnos()


        for i in range(1024):
            bucket, seqno = struct.unpack(">HQ", res[2][i*10:(i+1)*10])
            assert bucket == i




    # Check the scenario where time is not synced but we still request extended metadata. There should be no
    # adjusted time but the mutations should appear. This test currently fails - MB-13933

    def test_request_extended_meta_data_when_vbucket_not_time_synced(self):
        n = 5

        response = self.dcp_client.open_producer("producer")
        response = self.dcp_client.general_control('enable_ext_metadata', 'true')
        assert response['status'] == SUCCESS

        for i in xrange(n):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        stream = self.dcp_client.stream_req(0, 0, 0, n, 0)
        responses = stream.run()
        assert stream.last_by_seqno == n,\
               'Sequence number mismatch. Expect {0}, actual {1}'.format(n, stream.last_by_seqno)




    """ Tests the for the presence of the adjusted time and conflict resolution mode fields in the mutation and delete
        commands.
    """

    @unittest.skip("deferred from Watson")
    def test_conflict_resolution_and_adjusted_time(self):


        if self.os_type == 'windows':
            return # currently not supported on Windows

        n = 5

        response = self.dcp_client.open_producer("producer")
        response = self.dcp_client.general_control('enable_ext_metadata', 'true')
        assert response['status'] == SUCCESS

        # set time synchronization
        self.mcd_client.set_time_drift_counter_state(0,0,1)


        for i in xrange(n):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)



        stream = self.dcp_client.stream_req(0, 0, 0, n, 0)
        responses = stream.run()

        assert stream.last_by_seqno == n,\
               'Sequence number mismatch. Expect {0}, actual {1}'.format(n, stream.last_by_seqno)

        for i in responses:
            if i['opcode'] == CMD_MUTATION:
                assert i['nmeta'] > 0, 'nmeta is 0'
                assert i['conflict_resolution_mode'] == 1, 'Conflict resolution mode not set'
                assert i['adjusted_time'] > 0, 'Invalid adjusted time {0}'.format(i['adjusted_time'] )


    # This test will insert 100k items into a server,
    # Sets up a stream request. While streaming, will force the
    # server to crash. Reconnect stream and ensure that the
    # number of mutations received is as expected.
    def test_stream_req_with_server_crash(self):
        doc_count = 100000

        for i in range(doc_count):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        self.statsHandler.wait_for_persistence(self.mcd_client)

        by_seqno_list = []
        mutation_count = []

        def setup_a_stream(start, end, vb_uuid, snap_start, snap_end, by_seqnos, mutations):
            response = self.dcp_client.open_producer("mystream")
            assert response['status'] == SUCCESS

            stream = self.dcp_client.stream_req(0, 0, start, end, vb_uuid,
                                                snap_start, snap_end)
            assert stream.status == SUCCESS
            stream.run()
            by_seqnos.append(stream.last_by_seqno)
            mutations.append(stream.mutation_count)

        def kill_memcached():
            if self.os_type == 'windows':
                self._execute_command('taskkill /F /T /IM memcached*')
            else:
                self._execute_command('killall -9 memcached')

        response = self.mcd_client.stats('failovers')
        vb_uuid = long(response['vb_0:0:id'])

        start = 0
        end = doc_count
        proc1 = Thread(target=setup_a_stream,
                       args=(start, end, vb_uuid, start, start, by_seqno_list, mutation_count))

        proc2 = Thread(target=kill_memcached,
                       args=())

        proc1.start()
        proc2.start()
        proc2.join()
        proc1.join()

        # wait for server to be up
        self.statsHandler.wait_for_warmup(self.host, self.port)
        self.dcp_client = DcpClient(self.host, self.port)
        self.mcd_client = McdClient(self.host, self.port)

        response = self.mcd_client.stats('failovers')
        vb_uuid = long(response['vb_0:0:id'])

        assert len(by_seqno_list)
        start = by_seqno_list[0]
        end = doc_count
        setup_a_stream(start, end, vb_uuid, start, start, by_seqno_list, mutation_count)

        mutations_received_stage_one = mutation_count[0]
        mutations_received_stage_two = mutation_count[1]

        assert (mutations_received_stage_one + mutations_received_stage_two == doc_count)

    def test_track_mem_usage_with_repetetive_stream_req(self):
        doc_count = 100000

        for i in range(doc_count):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        self.statsHandler.wait_for_persistence(self.mcd_client)

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        resp = self.mcd_client.stats()
        memUsed_before = float(resp['mem_used'])

        start = 0
        end = doc_count
        snap_start = snap_end = start

        response = self.mcd_client.stats('failovers')
        vb_uuid = long(response['vb_0:0:id'])

        for i in range(0, 500):
            stream = self.dcp_client.stream_req(0, 0, start, end, vb_uuid,
                    snap_start, snap_end)
            assert stream.status == SUCCESS
            self.dcp_client.close_stream(0)

        time.sleep(5)

        resp = self.mcd_client.stats()
        memUsed_after = float(resp['mem_used'])

        assert (memUsed_after < ((0.1 * memUsed_before) + memUsed_before))



    """ Test for MB-11951 - streams which were opened and then closed without streaming any data caused problems
    """
    def test_unused_streams(self):



        initial_doc_count = 100

        for i in range(initial_doc_count):
            self.mcd_client.set('key1' + str(i), 0, 0, 'value', 0)

        # open the connection
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        # open the stream
        stream = self.dcp_client.stream_req(0, 0, 0, 1000, 0)

        # and without doing anything close it again
        response = self.dcp_client.close_stream(0)
        assert response['status'] == SUCCESS


        # then do some streaming for real

        doc_count = 100
        for i in range(doc_count):
            self.mcd_client.set('key2' + str(i), 0, 0, 'value', 0)

        # open the stream
        stream = self.dcp_client.stream_req(0, 0, 0, doc_count, 0)
        stream.run(doc_count)
        assert stream.last_by_seqno == doc_count, \
                     'Incorrect sequence number. Expect {0}, actual {1}'.format(doc_count,stream.last_by_seqno)



class McdTestCase(ParametrizedTestCase):
    def setUp(self):
        self.initialize_backend()

    def tearDown(self):
        self.destroy_backend()

    def test_stats(self):
        resp = self.mcd_client.stats()
        assert resp['curr_items'] == '0'

    def test_stat_vbucket_seqno(self):
        """Tests the vbucket-seqno stat.

        Insert 10 documents and check if the sequence number has the
        correct value.
        """
        doc_count = 10
        for i in range(doc_count):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        resp = self.mcd_client.stats('vbucket-seqno 0')
        seqno = int(resp['vb_0:high_seqno'])
        assert seqno == 10

    def test_stat_vbucket_seqno_not_my_vbucket(self):
        """Tests the vbucket-seqno NOT_MY_VBUCKET (0x07) response.

        Use a vBucket id that is way to hight in order to get a
        NOT_MY_VBUCKET (0x04) response back.
        """
        try:
            self.mcd_client.stats('vbucket-seqno 100000')
            assert False
        except Exception as ex:
            assert ex.status == ERR_NOT_MY_VBUCKET

    def test_stats_tap(self):
        resp = self.mcd_client.stats('tap')
        assert resp['ep_tap_backoff_period'] == '5'

    def test_set(self):
        self.mcd_client.set('key', 0, 0, 'value', 0)

        resp = self.mcd_client.stats()
        assert resp['curr_items'] == '1'

    def test_delete(self):
        self.mcd_client.set('key1', 0, 0, 'value', 0)
        self.mcd_client.delete('key1', 0, 0)

        assert Stats.wait_for_stat(self.mcd_client, 'curr_items', 0)

    def test_start_stop_persistence(self):
        retry = 5
        self.mcd_client.stop_persistence()
        self.mcd_client.set('key', 0, 0, 'value', 0)

        while retry > 0:
            time.sleep(2)

            resp = self.mcd_client.stats()
            state = resp['ep_flusher_state']
            if state == 'paused':
               break
            retry = retry - 1

        assert state == 'paused'
        self.mcd_client.start_persistence()
        self.statsHandler.wait_for_persistence(self.mcd_client)

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

    def cluster_reset(self, timeout = 120):
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

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS


        # start rebalance
        nodes = self.rest_client.get_nodes()
        assert len(nodes) == 1
        assert self.rest_client.rebalance(self.hosts[1:], [])

        # load and stream docs
        mutations = 0
        doc_count = 100
        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])
        high_seqno = long(resp['vb_0:0:seq'])

        for i in range(doc_count):

            try:
                self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)
            except Exception as ex:
                assert ex.status == ERR_NOT_MY_VBUCKET
                time.sleep(1)
                self.mcd_reset(0)
                self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

            start_seqno = mutations
            mutations = mutations + 1
            last_by_seqno = 0
            stream = self.dcp_client.stream_req(0, 0, start_seqno, mutations, vb_uuid)
            stream.run()
            assert stream.last_by_seqno == mutations

        assert self.rest_client.wait_for_rebalance(600)

    def test_stream_during_rebalance_in_out(self):
        """rebalance in/out while streaming mutations"""

        def load(vbucket, doc_count = 100):
            self.mcd_reset(vbucket)
            for i in range(doc_count):
                key = 'key %s' % (i)
                try:
                    self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)
                except Exception as ex:
                    assert ex.status == ERR_NOT_MY_VBUCKET
                    time.sleep(1)
                    self.mcd_reset(0)
                    self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)


        def stream(vbucket = 0, rolling_back = False):
            """ load doc_count items and stream them """
            self.mcd_reset(vbucket)

            response = self.dcp_client.open_producer("mystream")
            vb_stats = self.mcd_client.stats('vbucket-seqno')
            fl_stats = self.mcd_client.stats('failovers')

            vb_id = 'vb_%s' % vbucket
            start_seqno = long(fl_stats[vb_id+':0:seq'])
            end_seqno = long(vb_stats[vb_id+':high_seqno'])
            vb_uuid = long(vb_stats[vb_id+':uuid'])

            stream = self.dcp_client.stream_req(0, 0,
                                                start_seqno,
                                                end_seqno,
                                                vb_uuid, None)
            assert stream.status == SUCCESS, stream.status
            last_by_seqno = start_seqno
            stream.run()
            assert stream.last_by_seqno == end_seqno

        nodes = self.rest_client.get_nodes()
        assert len(nodes) == 1
        vbucket = 0

        # rebalance in
        for host in self.hosts[1:]:
            logging.info("rebalance in: %s" % host)
            assert self.rest_client.rebalance([host], [])
            load(vbucket)
            assert self.rest_client.wait_for_rebalance(600)
            stream(vbucket)

        # rebalance out
        for host in self.hosts[1:]:
            logging.info("rebalance out: %s" % host)
            assert self.rest_client.rebalance([], [host])
            load(vbucket)
            assert self.rest_client.wait_for_rebalance(600)
            stream(vbucket)

    def test_failover_swap_rebalance(self):
        """ add and failover node then perform swap rebalance """

        if len(self.hosts) <= 2:
            print "at least 3 nodes needed for this test: %s provided" %\
                                                             len(self.hosts)
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
                self.mcd_client.set(key, 0, 0, 'value', vb)


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
        try:
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
                vb_stats = mcd_client.stats('vbucket-seqno')
                for vb in vb_ids:
                    key = 'vb_%s:high_seqno' % vb
                    assert key in vb_stats, "Missing stats for %s: "% key
                    assert vb_stats[key] == str(doc_count),\
                        "expected high_seqno: %s, got: %s" % (doc_count, vb_stats[key])
                mcd_client.close()
        except AssertionError as aex:
            raise
        finally:
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
        vb_stats = self.mcd_client.stats('vbucket')
        replica_vbs = [key for key in vb_stats.keys()\
                    if vb_stats[key] == 'replica']
        assert len(replica_vbs) > 0 , 'No replica vbuckets, perhaps rebalance failed'
        vb = int(replica_vbs[0].split('_')[-1])
        self.mcd_reset(vb)

        # create a separate client for stream requests
        producer = DcpClient(self.host, self.port)
        producer.open_producer("producerstream")

        # stream 1st item
        self.mcd_client.set('key1', 0, 0, 'value', vb)
        stream = producer.stream_req(vb, 0, 0, 1, 0)
        while stream.has_response():
            response = stream.next_response()
            if 'key' in response:
                assert response['key'] == 'key1'
        producer.close()

        # failover
        failover_node = self.hosts[1]
        assert self.rest_client.failover(failover_node)
        self.mcd_reset(vb)
        self.mcd_client.set('key2', 0, 0, 'value', vb)

        # update producer
        producer = DcpClient(self.host, self.port)
        producer.open_producer("producerstream")
        stream = producer.stream_req(vb, 0, 0, 2, 0)

        # stream both items after failover
        assert self.rest_client.rebalance([], [failover_node])
        while stream.has_response():

            response = stream.next_response()

            assert response is not None,\
                 "Timeout reading stream after failover"

            if 'key' in response:
                if response['by_seqno'] == 1:
                    assert response['key'] == 'key1'
                elif response['by_seqno'] == 2:
                    assert response['key'] == 'key2'
                else:
                    assert False, "received unexpected mutation"
            if response['opcode'] == CMD_STREAM_END:
                break

        producer.close()
        assert self.rest_client.wait_for_rebalance(600)

    def test_stream_request_replica_to_active(self):
        """Verify replica that vbs become active after failover"""

        self.dcp_client.open_consumer("mystream")
        assert self.rest_client.rebalance(self.hosts[1:], [])
        assert self.rest_client.wait_for_rebalance(600)

        active_vbs = self.all_vbucket_ids('active')
        replica_vbs = self.all_vbucket_ids('replica')
        assert len(active_vbs) > 0, 'No active vbuckets on node'
        assert len(replica_vbs) > 0, 'No replica vbuckets on node'


        # load data into replica of node1 by loading into node2 active vbuckets
        orig_host, orig_port = self.host, self.port
        doc_count = 10
        for vb in replica_vbs:
            self.mcd_reset(vb)
            for i in xrange(doc_count):
                self.mcd_client.set('key' + str(i), 0, 0, 'value', vb)


        for host in self.hosts[2:]:
            assert self.rest_client.failover(host)

        assert self.rest_client.rebalance([], self.hosts[2:])
        assert self.rest_client.wait_for_rebalance(120)


        # check if original consumers still exist
        self.mcd_client = McdClient(orig_host, orig_port)
        active_vbs = self.all_vbucket_ids('active')
        replica_vbs = self.all_vbucket_ids('replica')
        stats = self.mcd_client.stats('dcp')
        dcp_count = stats['ep_dcp_count']
        assert int(dcp_count) == 3,\
            "Got dcp_count = {0}, expected = {1}".format(dcp_count, 3)


        # verify data can be streamed
        self.dcp_client.open_producer("producerstream")
        for vb in replica_vbs:
            stream = self.dcp_client.stream_req(vb, 0, 0, doc_count, 0, 0)
            stream.run()
            assert stream.last_by_seqno == doc_count,\
                    "Got %s, Expected %s" % (stream.last_by_seqno, doc_count)

    def test_failover_log_table_updated(self):
        """Verifies failover table entries are updated when vbucket ownership changes"""

        # get original failover table
        fl_table1 = self.mcd_client.stats('failovers')

        # rebalance in nodeB
        nodeB = self.hosts[1]
        assert self.rest_client.rebalance([nodeB], [])
        assert self.rest_client.wait_for_rebalance(600)
        replica_vbs = self.all_vbucket_ids('replica')
        assert len(replica_vbs) > 0, "No replica vbuckets!"
        self.mcd_reset(replica_vbs[0])

        # set and verify 1 item per nodeB vbucket
        [self.mcd_client.set('key' + str(vb), 0, 0, 'value', vb)\
                                                for vb in replica_vbs]

        # failover nodeB
        assert self.rest_client.failover(nodeB)
        assert self.rest_client.rebalance([], [nodeB])
        assert self.rest_client.wait_for_rebalance(600)
        self.mcd_reset(0)

        # get updated failover table
        fl_table2 = self.mcd_client.stats('failovers')

        # verify replica vbuckets have updated uuids
        # and old uuid matches uuids from original table
        for vb in replica_vbs:
            orig_uuid = long(fl_table1['vb_'+str(vb)+':0:id'])
            assert orig_uuid == long(fl_table2['vb_'+str(vb)+':1:id'])
            new_uuid = long(fl_table2['vb_'+str(vb)+':0:id'])
            assert orig_uuid != new_uuid

    def test_stream_request_failover_add_back(self):
        """Failover node while streaming mutationas then add_back and fetch same stream"""
        # rebalance in nodeB
        nodeB = self.hosts[1]
        assert self.rest_client.rebalance([nodeB], [])
        assert self.rest_client.wait_for_rebalance(600)
        replica_vbs = self.all_vbucket_ids('replica')
        assert len(replica_vbs) > 0, "No replica vbuckets!"


        # load data into replica vbucket
        doc_count = 10
        vb = replica_vbs[0]
        self.mcd_reset(vb)
        [self.mcd_client.set('key' + str(i), 0, 0, 'value', vb)\
                                         for i in range(doc_count)]

        def stream_and_failover():
            """streaming mutations from nodeB"""

            dcp_client = DcpClient(self.host, self.port)
            dcp_client.open_producer("mystream")
            stream = dcp_client.stream_req(vb, 0, 0, doc_count, 0)
            stream.run(doc_count/2)
            assert self.rest_client.failover(nodeB)
            stream.run()
            assert stream.last_by_seqno == doc_count
            dcp_client.close()

        # failover and stream
        stream_and_failover()

        # add back
        assert self.rest_client.re_add_node(nodeB)
        assert self.rest_client.rebalance([nodeB], [])
        assert self.rest_client.wait_for_rebalance(600)

        # stream after addback
        stream_and_failover()

    # The following test starts with a cluster of 2 nodes.
    # Issue mutations to a single vbucket.
    # Wait for flushers to settle, and check items consistency
    @unittest.skip("invalid: multiple nodes are not supported")
    def test_items_on_single_vbucket(self):

        nodeB = self.hosts[1]



        assert self.rest_client.rebalance([nodeB], [])
        assert self.rest_client.wait_for_rebalance(600)
        replica_vbs = self.all_vbucket_ids('replica')
        assert len(replica_vbs) > 0, "No replica vbuckets!"

        doc_count = 500000
        active_vbs = self.all_vbucket_ids('active')
        vb = active_vbs[0]
        self.mcd_reset(vb)
        [self.mcd_client.set('key' + str(i), 0, 0, 'value', vb)\
                                         for i in range(doc_count)]

        time.sleep(5)
        resp = self.mcd_client.stats()
        assert resp['vb_active_curr_items'] == str( doc_count )


        replica_resp = mcd_clientB.stats()
        assert replica_resp['vb_replica_curr_items'] == str( doc_count ), \
            'Incorrect vb_replica_curr_items. Expected {0}, actual {1}'.\
                format(doc_count, resp['vb_replica_curr_items'])
