from decorator import decorator

from fabric.api import hide, settings, parallel
from fabric.tasks import execute


@decorator
def multi_node_task(task, *args, **kargs):
    self = args[0]
    with settings(user=self.user, password=self.password, warn_only=True):
        with hide("running", "output"):
            return execute(parallel(task), *args, hosts=self.hosts, **kargs)


@decorator
def single_node_task(task, *args, **kargs):
    self = args[0]
    with settings(user=self.user, password=self.password, warn_only=True,
                  host_string=self.hosts[0]):
        with hide("running", "output"):
            return task(*args, **kargs)


class RemoteStats(object):

    def __init__(self, hosts, user, password):
        self.hosts = hosts
        self.user = user
        self.password = password
