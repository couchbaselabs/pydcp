import base64
import httplib2
import json
import time
import urllib

class RestClient(object):
    def __init__(self, ip, username='Administrator', password='password', port = 9000):

        self.ip = ip
        self.port = port
        self.username = username
        self.password = password
        self.baseUrl = "http://{0}:{1}/".format(self.ip, self.port)

    def _http_request(self, api, method = "GET", params='', timeout=120):
        auth_str = '%s:%s' % (self.username, self.password)
        headers =  {'Content-Type': 'application/x-www-form-urlencoded',
                    'Authorization': 'Basic %s' % base64.encodestring(auth_str),
                    'Accept': '*/*'}
        try:
            client = httplib2.Http(timeout=timeout)
            response, content = client.request(api, method, params, headers)
            if response['status'] in ['200', '201', '202']:
                return True, content, response
            else:
                return False, content, response
        except Exception as e:
            return False, e, ''

    def delete_bucket(self, bucket):
        api = '%s%s%s' % (self.baseUrl, 'pools/default/buckets/', bucket)
        status, content, header = self._http_request(api, 'DELETE')
        return status

    def create_default_bucket(self, replica = 1):
        params = urllib.urlencode({'name': 'default',
                                   'authType': 'sasl',
                                   'saslPassword': '',
                                   'ramQuotaMB': 256,
                                   'replicaNumber': replica,
                                   'proxyPort': 11211,
                                   'bucketType': 'membase',
                                   'replicaIndex': 1,
                                   'threadsNumber': 3,
                                   'flushEnabled': 1})
        api = '{0}{1}'.format(self.baseUrl, 'pools/default/buckets')
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def get_all_buckets(self):
        buckets = []
        api = '%s%s' % (self.baseUrl, 'pools/default/buckets')
        status, content, header = self._http_request(api, 'GET')
        json_parsed = json.loads(content)
        if status:
            for item in json_parsed:
                buckets.append(item['name'].encode('ascii'))
        return buckets


    def rebalance(self, nodes_in, nodes_out):


        nodesOtpMap = self.get_nodes_otp_map()
        ejectedNodes = [nodesOtpMap[n] for n in nodes_out if n in nodesOtpMap.keys()]
        knownNodes  = self.add_nodes(nodes_in)

        params = urllib.urlencode({'knownNodes': ','.join(knownNodes),
                                   'ejectedNodes': ','.join(ejectedNodes),
                                   'user': self.username,
                                   'password': self.password})

        api = self.baseUrl + "controller/rebalance"
        status, content, header = self._http_request(api, 'POST', params)
        if not status:
            print 'rebalance operation failed: {0}'.format(content)
            print params

        return status

    def add_nodes(self, nodes_in):
        """ and attempt to add new nodes to make known to cluster prior to rebalance
            and returns list of known nodes otp ids
        """
        known_nodes = self.get_nodes()

        for node in nodes_in:
            if node not in known_nodes:
                self.add_node(node)

        return self.get_nodes_otp_map().values()

    def add_node(self, addr ='', port='8091', retries = 5):

        otpNodeId = None
        addr_ = addr.split(':')
        remoteIp = addr_[0]
        if len(addr) > 1:
            port = addr_[1]

        params = urllib.urlencode({'hostname': "{0}:{1}".format(remoteIp, port),
                                   'user': self.username,
                                   'password': self.password})

        api = self.baseUrl + 'controller/addNode'
        status, content, header = self._http_request(api, 'POST', params)
        if status:
            json_parsed = json.loads(content)
            otpNodeId = json_parsed['otpNode']
        else:
            if retries > 0:
                retries -= 1
                time.sleep(5)
                self.add_node(addr, port, retries)
            else:
                print 'add_node error : {0}'.format(content)

        return otpNodeId

    def re_add_node(self, node):

        nodesOtpMap = self.get_nodes_otp_map()
        otpNode = nodesOtpMap.get(node)

        if otpNode is None:
            return False

        api = self.baseUrl + 'controller/reAddNode'
        params = urllib.urlencode({'otpNode': otpNode})
        status, content, header = self._http_request(api, 'POST', params)
        if not status:
            print 'add_back_node {0} error : {1}'.format(otpNode, content)

        return status

    def rebalance_statuses(self, bucket='default'):
        rebalanced = True
        api = self.baseUrl + 'pools/'+bucket+'/rebalanceProgress'
        status, content, header = self._http_request(api)
        if status:
            json_parsed = json.loads(content)
            if 'status' in json_parsed:
                if json_parsed['status'] == 'running':
                    rebalanced = False
        return rebalanced

    def wait_for_rebalance(self, timeout = 60):
        time.sleep(2)
        rebalanced = self.rebalance_statuses()
        while not rebalanced:
            time.sleep(2)
            timeout = timeout - 2
            if timeout == 0:
                print "Rebalance timed out"
                break
            rebalanced = self.rebalance_statuses()

        time.sleep(2)
        return rebalanced

    def get_bucket_info(self, bucket = 'default'):
        info = None
        api = self.baseUrl + 'pools/default/buckets/%s' % bucket
        status, content, header = self._http_request(api)

        if status:
            info = json.loads(content)

        return info

    def get_nodes_info(self):
        info = {}
        api = self.baseUrl + 'pools/default'
        status, content, header = self._http_request(api)

        if status:
            json_parsed = json.loads(content)
            info = json_parsed['nodes']

        return info

    def get_nodes_otp_map(self):
        nodes = {}

        for node in self.get_nodes_info():
            hostname = node['hostname']
            id_ = node['otpNode']
            nodes[hostname] = id_

        return nodes

    def get_nodes(self):
        return self.get_nodes_otp_map().keys()


    def init_self(self):
        api = self.baseUrl + 'settings/web'
        params = urllib.urlencode({'port': self.port,
                                   'username': self.username,
                                   'password': self.password})
        print 'settings/web params on {0}:{1}:{2}'.format(self.ip, self.port, params)
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def failover(self, node):
        nodes = self.get_nodes_otp_map()
        otpNode = nodes.get(node)
        status = False

        if otpNode:
            api = self.baseUrl + 'controller/failOver'
            params = urllib.urlencode({'otpNode': otpNode})
            status, content, header = self._http_request(api, 'POST', params)
            if not status:
                print 'fail_over node {0} error : {1}'.format(otpNode, content)

        return status

    def stop_rebalance(self, wait_timeout=10):
        api = self.baseUrl + '/controller/stopRebalance'
        status, content, header = self._http_request(api, 'POST')
        stopped = False
        if status:
            while wait_timeout > 0 and not stopped:
                stopped = self.rebalance_statuses()
                wait_timeout -= 1
                time.sleep(1)

        return stopped

if __name__ == "__main__":
    rest = RestClient('127.0.0.1')
    print rest.create_default_bucket()
    time.sleep(10)

    for bucket in rest.get_all_buckets():
        print rest.delete_bucket(bucket)
