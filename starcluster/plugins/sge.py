from starcluster import clustersetup
from starcluster.templates import sge
from starcluster.logger import log

from starcluster.balancers.sge import SGEStats
import re

class SGEPlugin(clustersetup.DefaultClusterSetup):

    def __init__(self, master_is_exec_host=True, scheduler_interval='0:0:15', master_slots=None, node_slots = None, **kwargs):
        self.master_is_exec_host = str(master_is_exec_host).lower() == "true"

        self._scheduler_interval = scheduler_interval
        if master_slots:
            self._master_slots = int(master_slots)
        if node_slots:
            self._node_slots = int(node_slots)
        else:
            self._node_slots = self._master_slots

        super(SGEPlugin, self).__init__(**kwargs)

    def _add_sge_submit_host(self, node):
        mssh = self._master.ssh
        mssh.execute('qconf -as %s' % node.alias)

    def _add_sge_admin_host(self, node):
        mssh = self._master.ssh
        mssh.execute('qconf -ah %s' % node.alias)

    def _setup_sge_profile(self, node):
        sge_profile = node.ssh.remote_file("/etc/profile.d/sge.sh", "w")
        arch = node.ssh.execute("/opt/sge6/util/arch")[0]
        sge_profile.write(sge.sgeprofile_template % dict(arch=arch))
        sge_profile.close()

    def _add_to_sge(self, node):
        node.ssh.execute('pkill -9 sge', ignore_exit_status=True)
        node.ssh.execute('rm /etc/init.d/sge*', ignore_exit_status=True)
        self._setup_sge_profile(node)
        self._inst_sge(node, exec_host=True)

    def _create_sge_pe(self, name="orte", nodes=None, queue="all.q"):
        """
        Create or update an SGE parallel environment

        name - name of parallel environment
        nodes - list of nodes to include in the parallel environment
                (default: all)
        queue - configure queue to use the new parallel environment
        """
        mssh = self._master.ssh
        pe_exists = mssh.get_status('qconf -sp %s' % name)
        pe_exists = pe_exists == 0
        verb = 'Updating'
        if not pe_exists:
            verb = 'Creating'
        log.info("%s SGE parallel environment '%s'" % (verb, name))
        # iterate through each machine and count the number of processors
        nodes = nodes or self._nodes
        num_processors = sum(self.pool.map(lambda n: n.num_processors, nodes,
                                           jobid_fn=lambda n: n.alias))
        penv = mssh.remote_file("/tmp/pe.txt", "w")
        penv.write(sge.sge_pe_template % (name, num_processors))
        penv.close()
        if not pe_exists:
            mssh.execute("qconf -Ap %s" % penv.name)
        else:
            mssh.execute("qconf -Mp %s" % penv.name)
        if queue:
            log.info("Adding parallel environment '%s' to queue '%s'" %
                     (name, queue))
            mssh.execute('qconf -mattr queue pe_list "%s" %s' % (name, queue))

    def _inst_sge(self, node, exec_host=True):
        inst_sge = 'cd /opt/sge6 && TERM=rxvt ./inst_sge '
        if node.is_master():
            inst_sge += '-m '
        if exec_host:
            inst_sge += '-x '
        inst_sge += '-noremote -auto ./ec2_sge.conf'
        node.ssh.execute(inst_sge, silent=True, only_printable=True)

    def _set_scheduler_interval(self, interval):
        log.info('Modifying scheduler interval')
        master=self._master
        cmd = "qconf -ssconf | sed s/0:0:15/" + self._scheduler_interval + "/ > /tmp/sched.conf.txt;"
        cmd += "qconf -Msconf /tmp/sched.conf.txt"
        master.ssh.execute(cmd, log_output=True,source_profile=True,raise_on_failure=False)

    def _set_number_slots(self, master_slots, node_slots):
        log.info('Modifying number of master & node slots')
        master=self._master
        cmd = "qconf -sq all.q | sed s/=[0-9]*]/=" + str(node_slots) +"]/g > /tmp/queue.conf.txt;"
        #cmd += "qconf -Mq /tmp/queue.conf.txt;"
        cmd += "cat /tmp/queue.conf.txt | sed s/master=[0-9]*]/master=" + str(master_slots) +"]/ > /tmp/queue.conf2.txt;"
        cmd += "qconf -Mq /tmp/queue.conf2.txt;"
        master.ssh.execute(cmd, log_output=True,source_profile=True,raise_on_failure=False)

    def _get_number_of_slots(self,node):
        """
        get number of slots for the given node by parsing qconf -sq all.q
        """

        qconf_output = '\n'.join(self._master.ssh.execute('qconf -sq all.q'))
        slots =  int(re.search("%s=[0-9]+" % node.alias,qconf_output).group(0).split('=')[1])

        return slots

    def _set_fill_up_host(self):
        """
        normally it's np_load_avg, this needs to be run per execution host
        """

        log.info('Setting scheduler to FILL UP HOST')
        master = self._master
        nodes = self.nodes

        print [master] + nodes
        for node in [master] + nodes:
            slots = self._get_number_of_slots(node)

            qconf_str = """
hostname              %s
load_scaling          NONE
complex_values        slots=%s
user_lists            NONE
xuser_lists           NONE
projects              NONE
xprojects             NONE
usage_scaling         NONE
report_variables      NONE
""" % (node.alias, slots)

            cmd = 'echo "%s" > /tmp/host.conf.txt;' % qconf_str
            cmd += 'qconf -Me /tmp/host.conf.txt;'
            node.ssh.execute(cmd, log_output=False,source_profile=True,raise_on_failure=False)

        cmd = 'qconf -ssconf | sed "s/load_formula.*/load_formula  slots/" > /tmp/sched.conf.txt;'
        cmd += 'qconf -Msconf /tmp/sched.conf.txt;'
        master.ssh.execute(cmd, log_output=False,source_profile=True,raise_on_failure=False)

    def _clear_error_states(self):
        """
        Used do clear up error states on jobs that may occur for some reason or other
        """

        log.info('Clearing any error states on queue')
        master=self._master
        qstat_cmd = 'qstat -u \* -xml'
        qstatxml = '\n'.join(master.ssh.execute(qstat_cmd))
        stats = SGEStats()
        qstat =  stats.parse_qstat(qstatxml, queues=["all.q", ""])

        log.info("%s" % qstat)
        Eqw = filter(lambda x: re.match(r'^Eqw',x.get('state','')),qstat)
        log.info("%s" % Eqw)
        if Eqw:
            jobs = {}
            for j in Eqw:
                log.info("---> %s" % j)
                jobs[(j['JB_job_number'],j.get('tasks',None))] = 1
            jobs = jobs.keys()
            log.info("%s" % jobs)
            for j in jobs:
                log.info("------> %s" % str(j))
                if j[1]:
                    cmd = 'qmod -cj %s -t %s' % (j[0],j[1])
                else:
                    cmd = 'qmod -cj %s' % j[0]
                log.info("cmd: %s" % cmd)

                master.ssh.execute(cmd)

        # TODO: for some reason re-adding a node can result in that node being disabled, this is a quick fix
        cmd = 'qmod -e all.q'
        master.ssh.execute(cmd)

    def _setup_sge(self):
        """
        Install Sun Grid Engine with a default parallel
        environment on StarCluster
        """
        master = self._master
        if not master.ssh.isdir('/opt/sge6'):
            # copy fresh sge installation files to /opt/sge6
            master.ssh.execute('cp -r /opt/sge6-fresh /opt/sge6')
            master.ssh.execute('chown -R %(user)s:%(user)s /opt/sge6' %
                               {'user': self._user})
        self._setup_nfs(self.nodes, export_paths=['/opt/sge6'],
                        start_server=False)
        # setup sge auto install file
        default_cell = '/opt/sge6/default'
        if master.ssh.isdir(default_cell):
            log.info("Removing previous SGE installation...")
            master.ssh.execute('rm -rf %s' % default_cell)
            master.ssh.execute('exportfs -fr')
        admin_hosts = ' '.join(map(lambda n: n.alias, self._nodes))
        submit_hosts = admin_hosts
        exec_hosts = admin_hosts
        ec2_sge_conf = master.ssh.remote_file("/opt/sge6/ec2_sge.conf", "w")
        conf = sge.sgeinstall_template % dict(admin_hosts=admin_hosts,
                                              submit_hosts=submit_hosts,
                                              exec_hosts=exec_hosts)
        ec2_sge_conf.write(conf)
        ec2_sge_conf.close()
        log.info("Installing Sun Grid Engine...")
        self._inst_sge(master, exec_host=self.master_is_exec_host)
        self._setup_sge_profile(master)
        # set all.q shell to bash
        master.ssh.execute('qconf -mattr queue shell "/bin/bash" all.q')
        for node in self.nodes:
            self._add_sge_admin_host(node)
            self._add_sge_submit_host(node)
            self.pool.simple_job(self._add_to_sge, (node,), jobid=node.alias)
        self.pool.wait(numtasks=len(self.nodes))
        self._create_sge_pe()


    def _resubmit_node_jobs(self, node):
        log.info('Resubmitting any running jobs')
        master = self._master
        stats = SGEStats()
        qstat_cmd = "qstat  -s r -u asrproc -q [ all.q@%s ] -xml" % node._alias
        qstatxml = '\n'.join(master.ssh.execute(qstat_cmd, log_output=False,
            source_profile=True,
            raise_on_failure=True))

        if qstatxml:
            qstat =  stats.parse_qstat(qstatxml, queues=["all.q", ""])

            jobs = {}
            for j in qstat:
                jobs[j['JB_job_number']] = 1
            jobs = jobs.keys()

            for j in jobs:
                qalter_cmd = 'qalter -r y %s' % j
                master.ssh.execute(qalter_cmd, log_output=False, source_profile=True, raise_on_failure=True)

            for j in qstat:
                # resubmit those jobs
                if 'tasks' in j:
                    qmod_cmd = "qmod -r %s.%s" % (j["JB_job_number"],j["tasks"])
                    master.ssh.execute(qmod_cmd, log_output=False, source_profile=True, raise_on_failure=True)
                else:
                    qmod_cmd = "qmod -r %s" % (j["JB_job_number"])
                    master.ssh.execute(qmod_cmd, log_output=False, source_profile=True, raise_on_failure=True)

    def _remove_from_sge(self, node):
        master = self._master
        master.ssh.execute('qconf -dattr hostgroup hostlist %s @allhosts' %
                           node.alias)
        master.ssh.execute('qconf -purge queue slots all.q@%s' % node.alias)
        master.ssh.execute('qconf -dconf %s' % node.alias)
        master.ssh.execute('qconf -de %s' % node.alias)
        node.ssh.execute('pkill -9 sge_execd')
        nodes = filter(lambda n: n.alias != node.alias, self._nodes)
        self._create_sge_pe(nodes=nodes)

    def run(self, nodes, master, user, user_shell, volumes):
        if not master.ssh.isdir("/opt/sge6-fresh"):
            log.error("SGE is not installed on this AMI, skipping...")
            return
        log.info("Configuring SGE...")
        self._nodes = nodes
        self._master = master
        self._user = user
        self._user_shell = user_shell
        self._volumes = volumes
        self._setup_sge()

        self._set_scheduler_interval(self._scheduler_interval)
        if self._master_slots:
            self._set_number_slots(self._master_slots, self._node_slots)
        self._set_fill_up_host()
    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        self._nodes = nodes
        self._master = master
        self._user = user
        self._user_shell = user_shell
        self._volumes = volumes
        log.info("Adding %s to SGE" % node.alias)
        self._setup_nfs(nodes=[node], export_paths=['/opt/sge6'],
                        start_server=False)
        self._add_sge_admin_host(node)
        self._add_sge_submit_host(node)
        self._add_to_sge(node)
        self._create_sge_pe()

        if self._master_slots:
            self._set_number_slots(self._master_slots, self._node_slots)
        self._set_fill_up_host()
    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        self._nodes = nodes
        self._master = master
        self._user = user
        self._user_shell = user_shell
        self._volumes = volumes
        log.info("Removing %s from SGE" % node.alias)
        self._remove_from_sge(node)
        self._remove_nfs_exports(node)
        self._resubmit_node_jobs(node)
