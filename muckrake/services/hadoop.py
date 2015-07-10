# Copyright 2014 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.services.service import Service

from muckrake.defaults import DEFAULT_JDK

import time
import abc
import os.path


def create_hadoop_service(context, num_nodes, hadoop_distro, hadoop_version, jdk=DEFAULT_JDK):

    if hadoop_distro == 'cdh':
        hadoop_home = '/opt/hadoop-cdh/'
        if hadoop_version == 1:
            return CDHV1Service(context, num_nodes, hadoop_home, jdk)
        else:
            return CDHV2Service(context, num_nodes, hadoop_home, jdk)
    else:
        hadoop_home = '/usr/hdp/current/hadoop-hdfs-namenode/../hadoop/'
        return HDPService(context, num_nodes, hadoop_home, jdk)


class HDFSService(Service):

    logs = {
        "hadoop_logs": {
            "path": "/mnt/logs",
            "collect_default": False
        }
    }

    def __init__(self, context, num_nodes, hadoop_home, hadoop_distro, jdk=DEFAULT_JDK):
        """
        :type context
        :type hadoop_home: str
        :type hadoop_distro: str
        """
        super(HDFSService, self).__init__(context, num_nodes)
        self.master_host = None
        self.slaves = []
        self.hadoop_home = hadoop_home
        self.hadoop_distro = hadoop_distro
        self.hadoop_bin_dir = 'bin'
        self.hadoop_example_jar = None
        self.jdk = jdk
        self.java_home = '/usr/lib/jvm/java-%d-oracle' % self.jdk

    def start(self):
        """Override Service.start
        This lets us bring HDFS on all nodes before bringing up hadoop.
        """

        # Since we're overriding start, we need to manually allocate nodes
        self.allocate_nodes()

        for idx, node in enumerate(self.nodes, 1):
            if idx == 1:
                self.master_host = node.account.hostname

            self.logger.info("Stopping HDFS on %s", node.account.hostname)
            self.stop_node(node)
            self.clean_node(node)

            self.create_hdfs_dirs(node)
            self.distribute_hdfs_confs(node)

            if idx == 1:
                self.format_namenode(node)
                self.start_namenode(node)
            else:
                self.slaves.append(node.account.hostname)
                self.start_datanode(node)
            time.sleep(5)  # wait for start up

    def stop_node(self, node):
        node.account.kill_process("java", clean_shutdown=False)

    def clean_node(self, node):
        self.logger.info("Removing HDFS directories on %s", node.account.hostname)
        node.account.ssh("rm -rf /mnt/data/ /mnt/name/ /mnt/logs /mnt/hadoop-env.sh /mnt/core-site.xml /mnt/hdfs-site.xml")

    def create_hdfs_dirs(self, node):
        self.logger.info("Creating hdfs directories on %s", node.account.hostname)
        node.account.ssh("mkdir -p /mnt/data")
        node.account.ssh("mkdir -p /mnt/name")
        node.account.ssh("mkdir -p /mnt/logs")

    def template(self, filename):
        return os.path.join(self.hadoop_distro, filename)

    def distribute_hdfs_confs(self, node):
        self.logger.info("Distributing hdfs confs to %s", node.account.hostname)

        hadoop_env = self.render(self.template('hadoop-env.sh'), java_home=self.java_home)
        core_site = self.render(self.template('core-site.xml'))
        hdfs_site = self.render(self.template('hdfs-site.xml'),
                                dfs_replication=1, dfs_name_dir='/mnt/name', dfs_data_dir='/mnt/data')

        node.account.create_file("/mnt/hadoop-env.sh", hadoop_env)
        node.account.create_file("/mnt/core-site.xml", core_site)
        node.account.create_file("/mnt/hdfs-site.xml", hdfs_site)

    @abc.abstractmethod
    def distribute_mr_confs(self, node):
        return

    def format_namenode(self, node):
        self.logger.info("Formatting namenode on %s", node.account.hostname)
        node.account.ssh("HADOOP_CONF_DIR=/mnt " + self.hadoop_home + "bin/hadoop namenode -format")

    def start_namenode(self, node):
        self.logger.info("Starting namenode on %s", node.account.hostname)
        node.account.ssh(
            "HADOOP_LOG_DIR=/mnt/logs " + self.hadoop_home + "/sbin/hadoop-daemon.sh "
            "--config /mnt/ start namenode")

    def start_datanode(self, node):
        self.logger.info("Starting datanode on %s", node.account.hostname)
        node.account.ssh(
            "HADOOP_LOG_DIR=/mnt/logs " + self.hadoop_home + "/sbin/hadoop-daemon.sh "
            "--config /mnt/ start datanode")


class CDHV1Service(HDFSService):
    def __init__(self, context, num_nodes, hadoop_home, jdk):
        """
        :type context
        :type hadoop_home: str
        """
        super(CDHV1Service, self).__init__(context, num_nodes, hadoop_home, 'cdh', jdk)
        self.hadoop_bin_dir = 'bin-mapreduce1'
        self.hadoop_example_jar = self.hadoop_home + \
            'share/hadoop/mapreduce1/hadoop-examples-2.5.0-mr1-cdh5.3.0.jar'

    def start(self):
        super(CDHV1Service, self).start()

        for idx, node in enumerate(self.nodes, 1):
            self.logger.info("Stopping MRv1 on %s", node.account.hostname)
            self._stop_and_clean(node, allow_fail=True)

            self.distribute_mr_confs(node)

            if idx == 1:
                self.start_jobtracker(node)
                self.start_jobhistoryserver(node)
            else:
                self.start_tasktracker(node)
            time.sleep(5)

    def stop_node(self, node):
        super(CDHV1Service, self).stop_node(node)
        node.account.ssh("HADOOP_LOG_DIR=/mnt/logs " + self.hadoop_home +
                         "/bin-mapreduce1/hadoop-daemon.sh --config /mnt "
                         "stop tasktracker", allow_fail=True)
        node.account.ssh("HADOOP_LOG_DIR=/mnt/logs " + self.hadoop_home +
                         "/bin-mapreduce1/hadoop-daemon.sh --config /mnt "
                         "stop jobtracker", allow_fail=True)
        node.account.ssh("HADOOP_MAPRED_LOG_DIR=/mnt/logs " + self.hadoop_home +
                         "sbin/mr-jobhistory-daemon.sh --config /mnt "
                         "stop historyserver", allow_fail=True)

    def clean_node(self, node):
        super(CDHV1Service, self).clean_node(node)
        self.logger.debug("Removing CDH files")
        node.account.ssh("rm -rf /mnt/mapred-site.xml /mnt/hadoop-metrics.properties", allow_fail=True)

    def distribute_mr_confs(self, node):
        self.logger.info("Distributing MR1 confs to %s", node.account.hostname)
        node.account.create_file("/mnt/mapred-site.xml", self.render(self.template('mapred-site.xml')))
        node.account.ssh("cp " + self.hadoop_home + "etc/hadoop-mapreduce1/hadoop-metrics.properties /mnt")

    def start_jobtracker(self, node):
        self.logger.info("Starting jobtracker on %s", node.account.hostname)
        node.account.ssh("HADOOP_LOG_DIR=/mnt/logs " + self.hadoop_home +
                         "/bin-mapreduce1/hadoop-daemon.sh --config /mnt "
                         "start jobtracker &")

    def start_tasktracker(self, node):
        self.logger.info("Starting tasktracker on %s", node.account.hostname)
        node.account.ssh("HADOOP_LOG_DIR=/mnt/logs " + self.hadoop_home +
                         "/bin-mapreduce1/hadoop-daemon.sh --config /mnt "
                         "start tasktracker &")

    def start_jobhistoryserver(self, node):
        self.logger.info("Starting job history server on %s", node.account.hostname)
        node.account.ssh("HADOOP_MAPRED_LOG_DIR=/mnt/logs " + self.hadoop_home +
                         "/sbin/mr-jobhistory-daemon.sh --config /mnt "
                         "start historyserver &")

    def _stop_and_clean(self, node, allow_fail=False):
        node.account.ssh("HADOOP_LOG_DIR=/mnt/logs " + self.hadoop_home +
                         "/bin-mapreduce1/hadoop-daemon.sh --config /mnt "
                         "stop tasktracker", allow_fail=allow_fail)
        node.account.ssh("HADOOP_LOG_DIR=/mnt/logs " + self.hadoop_home +
                         "/bin-mapreduce1/hadoop-daemon.sh --config /mnt "
                         "stop jobtracker", allow_fail=allow_fail)
        node.account.ssh("HADOOP_MAPRED_LOG_DIR=/mnt/logs " + self.hadoop_home +
                         "sbin/mr-jobhistory-daemon.sh --config /mnt "
                         "stop historyserver", allow_fail=allow_fail)
        time.sleep(5)  # the stop script doesn't wait
        node.account.ssh("rm -rf /mnt/mapred-site.xml")


class CDHV2Service(HDFSService):
    def __init__(self, context, num_nodes, hadoop_home, jdk):
        """
        :type context
        :type hadoop_home: str
        """
        super(CDHV2Service, self).__init__(context, num_nodes, hadoop_home, 'cdh', jdk)
        self.hadoop_example_jar = self.hadoop_home + \
            'share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar'

    def start(self):
        super(CDHV2Service, self).start()

        for idx, node in enumerate(self.nodes, 1):
            self.logger.info("Stopping YARN on %s", node.account.hostname)
            self._stop_and_clean(node, allow_fail=True)

            self.distribute_mr_confs(node)

            if idx == 1:
                self.start_resourcemanager(node)
                self.start_jobhistoryserver(node)
            else:
                self.start_nodemanager(node)
            time.sleep(5)

    def stop_node(self, node):
        super(CDHV2Service, self).stop_node(node)
        node.account.ssh("YARN_LOG_DIR=/mnt/logs " + self.hadoop_home + "/sbin/yarn-daemon.sh --config /mnt "
            "stop nodemanager &", allow_fail=True)
        node.account.ssh("YARN_LOG_DIR=/mnt/logs " + self.hadoop_home + "/sbin/yarn-daemon.sh --config /mnt "
            "stop resourcemanager &", allow_fail=True)
        node.account.ssh("HADOOP_MAPRED_LOG_DIR=/mnt/logs " + self.hadoop_home +
                         "/sbin/mr-jobhistory-daemon.sh --config /mnt "
                         "stop historyserver", allow_fail=True)
        time.sleep(5)

    def clean_node(self, node):
        super(CDHV2Service, self).clean_node(node)
        node.account.ssh("rm -rf /mnt/hadoop-metrics.properties /mnt/yarn-site.xml /mnt/mapred-site.xml /mnt/yarn-env.sh", allow_fail=True)

    def distribute_mr_confs(self, node):
        self.logger.info("Distributing YARN confs to %s", node.account.hostname)

        yarn_env = self.render(self.template('yarn-env.sh'), java_home=self.java_home)

        node.account.create_file("/mnt/mapred-site.xml", self.render(self.template('mapred2-site.xml')))
        node.account.create_file("/mnt/yarn-env.sh", yarn_env)
        node.account.create_file("/mnt/yarn-site.xml", self.render(self.template('yarn-site.xml')))
        node.account.ssh("cp " + self.hadoop_home + "/etc/hadoop/hadoop-metrics.properties /mnt")

    def start_resourcemanager(self, node):
        self.logger.info("Starting ResourceManager on %s", node.account.hostname)
        node.account.ssh("YARN_LOG_DIR=/mnt/logs " + self.hadoop_home + "/sbin/yarn-daemon.sh --config /mnt "
                         "start resourcemanager &")

    def start_nodemanager(self, node):
        self.logger.info("Starting NodeManager on %s", node.account.hostname)
        node.account.ssh("YARN_LOG_DIR=/mnt/logs " + self.hadoop_home + "/sbin/yarn-daemon.sh --config /mnt "
                         "start nodemanager &")

    def start_jobhistoryserver(self, node):
        self.logger.info("Start job history server on %s", node.account.hostname)
        node.account.ssh("HADOOP_MAPRED_LOG_DIR=/mnt/logs " + self.hadoop_home +
                         "/sbin/mr-jobhistory-daemon.sh --config /mnt "
                         "start historyserver &")

    def _stop_and_clean(self, node, allow_fail=False):
        node.account.ssh("YARN_LOG_DIR=/mnt/logs " + self.hadoop_home + "/sbin/yarn-daemon.sh --config /mnt "
            "stop nodemanager &", allow_fail=allow_fail)
        node.account.ssh("YARN_LOG_DIR=/mnt/logs " + self.hadoop_home + "/sbin/yarn-daemon.sh --config /mnt "
            "stop resourcemanager &", allow_fail=allow_fail)
        node.account.ssh("HADOOP_MAPRED_LOG_DIR=/mnt/logs " + self.hadoop_home +
                         "/sbin/mr-jobhistory-daemon.sh --config /mnt "
                         "stop historyserver", allow_fail=allow_fail)
        time.sleep(5)  # the stop script doesn't wait
        node.account.ssh("rm -rf /mnt/yarn-site.xml /mnt/mapred-site.xml /mnt/yarn-env.sh")


class HDPService(HDFSService):
    def __init__(self, context, num_nodes, hadoop_home, jdk):
        """
        :type context
        :type hadoop_home: str
        """
        super(HDPService, self).__init__(context, num_nodes, hadoop_home, 'hdp', jdk)
        self.hadoop_example_jar = '/usr/hdp/current/hadoop-mapreduce-client/hadoop-mapreduce-examples-*.jar'
        self.yarn_bin_path = '/usr/hdp/current/hadoop-yarn-resourcemanager/'
        self.hdfs_bin_path = '/usr/hdp/current/hadoop-hdfs-namenode/'
        self.historyserver_bin_path = '/usr/hdp/current/hadoop-mapreduce-historyserver/'
        self.hadoop_client_bin_path = '/usr/hdp/current/hadoop-client/'

    def start(self):
        super(HDPService, self).start()

        for idx, node in enumerate(self.nodes, 1):
            self.logger.info("Stopping YARN on %s", node.account.hostname)
            self._stop_and_clean(node, allow_fail=True)

            self.distribute_mr_confs(node)

            if idx == 1:
                self.config_on_hdfs(node)
                self.start_resourcemanager(node)
                self.start_jobhistoryserver(node)
            else:
                self.start_nodemanager(node)
            time.sleep(5)

    def stop_node(self, node):
        super(HDPService, self).stop_node(node)
        node.account.ssh("YARN_LOG_DIR=/mnt/logs " + self.yarn_bin_path + "sbin/yarn-daemon.sh "
                         "--config /mnt "
                         "stop nodemanager", allow_fail=True)
        node.account.ssh("YARN_LOG_DIR=/mnt/logs " + self.yarn_bin_path + "sbin/yarn-daemon.sh "
                         "--config /mnt "
                         "stop resourcemanager", allow_fail=True)
        node.account.ssh("HADOOP_MAPRED_LOG_DIR=/mnt/logs "
                         + self.historyserver_bin_path + "sbin/mr-jobhistory-daemon.sh"
                         " --config /mnt stop historyserver", allow_fail=True)

    def clean_node(self, node):
        super(HDPService, self).clean_node(node)
        self.logger.debug("Removing HDP files on " + node.account.hostname)
        node.account.ssh("rm -rf /mnt/hadoop-metrics.properties /mnt/yarn-site.xml /mnt/mapred-site.xml /mnt/yarn-env.sh /mnt/capacity-scheduler.xml", allow_fail=True)

    def config_on_hdfs(self, node):
        self.logger.info("Make necessary YARN configuration in HDFS at %s", node.account.hostname)
        node.account.ssh(
            self.hdfs_bin_path + "bin/hdfs dfs -mkdir -p /hdp/apps/2.2.0.0-2041/mapreduce/"
        )

        node.account.ssh(
            self.hdfs_bin_path + "/bin/hdfs dfs -put " +
            self.hadoop_client_bin_path + "mapreduce.tar.gz "
            "/hdp/apps/2.2.0.0-2041/mapreduce/"
        )

        node.account.ssh(
            self.hdfs_bin_path + "/bin/hdfs dfs "
            "-chown -R hdfs:hadoop /hdp"
        )

        node.account.ssh(
            self.hdfs_bin_path + "/bin/hdfs dfs "
            "-chmod -R 555 /hdp/apps/2.2.0.0-2041/mapreduce"
        )

    def distribute_mr_confs(self, node):
        self.logger.info("Distributing YARN confs to %s", node.account.hostname)

        yarn_env = self.render(self.template('yarn-env.sh'), java_home=self.java_home)

        node.account.create_file("/mnt/mapred-site.xml", self.render(self.template('mapred-site.xml')))
        node.account.create_file("/mnt/yarn-site.xml", self.render(self.template('yarn-site.xml')))
        node.account.create_file("/mnt/yarn-env.sh", yarn_env)
        node.account.ssh("cp /etc/hadoop/conf/hadoop-metrics.properties /mnt")
        node.account.ssh("cp /etc/hadoop/conf/capacity-scheduler.xml /mnt")

    def start_resourcemanager(self, node):
        self.logger.info("Starting ResourceManager on %s", node.account.hostname)
        node.account.ssh("YARN_LOG_DIR=/mnt/logs " + self.yarn_bin_path + "sbin/yarn-daemon.sh "
                         "--config /mnt "
                         "start resourcemanager")

    def start_nodemanager(self, node):
        self.logger.info("Starting NodeManager on %s", node.account.hostname)
        node.account.ssh("YARN_LOG_DIR=/mnt/logs " + self.yarn_bin_path + "sbin/yarn-daemon.sh "
                         "--config /mnt "
                         "start nodemanager")

    def start_jobhistoryserver(self, node):
        self.logger.info("Start job history server on %s", node.account.hostname)
        node.account.ssh("HADOOP_MAPRED_LOG_DIR=/mnt/logs "
                         + self.historyserver_bin_path + "sbin/mr-jobhistory-daemon.sh"
                         " --config /mnt start historyserver")

    def _stop_and_clean(self, node, allow_fail=False):
        node.account.ssh("YARN_LOG_DIR=/mnt/logs " + self.yarn_bin_path + "sbin/yarn-daemon.sh "
                         "--config /mnt "
                         "stop nodemanager", allow_fail=allow_fail)
        node.account.ssh("YARN_LOG_DIR=/mnt/logs " + self.yarn_bin_path + "sbin/yarn-daemon.sh "
                         "--config /mnt "
                         "stop resourcemanager", allow_fail=allow_fail)
        node.account.ssh("HADOOP_MAPRED_LOG_DIR=/mnt/logs "
                         + self.historyserver_bin_path + "sbin/mr-jobhistory-daemon.sh"
                         " --config /mnt stop historyserver", allow_fail=allow_fail)
        time.sleep(5)  # the stop script doesn't wait
        node.account.ssh("rm -rf /mnt/yarn-site.xml /mnt/mapred-site.xml /mnt/yarn-env.sh")

