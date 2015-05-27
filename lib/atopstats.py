from uuid import uuid4

from fabric.api import run

from remotestats import RemoteStats, multi_node_task, single_node_task


class AtopStats(RemoteStats):

    def __init__(self, os_type, hosts, user, password):
        super(AtopStats, self).__init__(hosts, user, password)
        self.os_type = os_type
        if self.os_type == 'windows': return
        self.logfile = "/tmp/{}.atop".format(uuid4().hex)

        self._base_cmd =\
            "d=`date +%H:%M` && atop -r {} -b $d -e $d".format(self.logfile)

    @multi_node_task
    def stop_atop(self):
        if self.os_type == 'windows': return
        run("killall -q atop")
        run("rm -rf /tmp/*.atop")

    @multi_node_task
    def start_atop(self):
        if self.os_type == 'windows': return
        run("nohup atop -a -w {} 5 > /dev/null 2>&1 &".format(self.logfile),
            pty=False)

    @single_node_task
    def update_columns(self):
        if self.os_type == 'windows': return
        self._cpu_column = self._get_cpu_column()
        self._vsize_column = self._get_vsize_column()
        self._rss_column = self._get_rss_column()

    def restart_atop(self):
        if self.os_type == 'windows': return
        self.stop_atop()
        self.start_atop()

    @single_node_task
    def _get_vsize_column(self):
        if self.os_type == 'windows': return
        output = run("atop -m 1 1 | grep PID")
        return output.split().index("VSIZE")

    @single_node_task
    def _get_rss_column(self):
        if self.os_type == 'windows': return
        output = run("atop -m 1 1 | grep PID")
        return output.split().index("RSIZE")

    @single_node_task
    def _get_cpu_column(self):
        if self.os_type == 'windows': return
        output = run("atop 1 1 | grep PID")
        return output.split().index("CPU")

    @staticmethod
    def _get_metric(cmd, column):
        result = run(cmd)
        if not result.return_code:
            return result.split()[column]
        else:
            return None

    @multi_node_task
    def get_process_cpu(self, process):
        if self.os_type == 'windows': return
        title = process + "_cpu"
        cmd = "{} | grep {}".format(self._base_cmd, process)
        return title, self._get_metric(cmd, self._cpu_column)

    @multi_node_task
    def get_process_vsize(self, process):
        if self.os_type == 'windows': return
        title = process + "_vsize"
        cmd = "{} -m | grep {}".format(self._base_cmd, process)
        return title, self._get_metric(cmd, self._vsize_column)

    @multi_node_task
    def get_process_rss(self, process):
        if self.os_type == 'windows': return
        title = process + "_rss"
        cmd = "{} -m | grep {}".format(self._base_cmd, process)
        return title, self._get_metric(cmd, self._rss_column)
