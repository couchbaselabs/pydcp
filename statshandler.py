
import time

from constants import *
from lib.mc_bin_client import MemcachedClient as McdClient

class Stats():

    def __init__(self, bucket_type):
        self.bucket_type = bucket_type

    @staticmethod
    def get_stat(client, stat, type=''):
        response = client.stats(type)
        return response[stat]

    @staticmethod
    def check(client, stat, exp, type=''):
        op = client.stats(type)
        response = op.next_response()
        assert response['status'] == SUCCESS
        assert response['value'][stat] == exp

    @staticmethod
    def exists(client, stat, type=''):
        op = client.stats(type)
        response = op.next_response()
        assert response['status'] == SUCCESS
        return stat in response['value']


    def wait_for_persistence(self, client):
        if self.bucket_type == 'ephemeral': return

        while int(Stats.get_stat(client, 'ep_queue_size')) > 0:
            time.sleep(1)
        while int(Stats.get_stat(client, 'ep_commit_num')) == 0:
            time.sleep(1)

    @staticmethod
    def wait_for_stat(client, stat, val, type=''):
        for i in range(60):
            resp = client.stats(type)
            assert stat in resp
            if resp[stat] == str(val):
                return True
            time.sleep(1)
        return False

    def wait_for_warmup(self, host, port):
        if self.bucket_type == 'ephemeral': return
        while True:
            client = McdClient(host, port)
            try:
                client.bucket_select("default")
                response = client.stats()
                # check the old style or new style (as of 4.5) results
                mode = response.get('ep_degraded_mode')
                if  mode is not None:
                    if mode == '0' or mode == 'false':
                        break
            except Exception as ex:
                pass
            time.sleep(1)
