import base64
import httplib2
import json
import time
import urllib

class RestClient(object):
    def __init__(self, ip, username='Administrator', password='password'):
        self.ip = ip
        self.username = username
        self.password = password
        self.port = 9000
        self.baseUrl = "http://{0}:{1}/".format(self.ip, self.port)

    def _http_request(self, api, method, params='', timeout=120):
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

    def create_default_bucket(self):
        params = urllib.urlencode({'name': 'default',
                                   'authType': 'sasl',
                                   'saslPassword': '',
                                   'ramQuotaMB': 256,
                                   'replicaNumber': 1,
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

if __name__ == "__main__":
    rest = RestClient('127.0.0.1')
    print rest.create_default_bucket()
    time.sleep(10)
    
    for bucket in rest.get_all_buckets():
        print rest.delete_bucket(bucket)
