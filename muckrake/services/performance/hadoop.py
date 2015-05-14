# Copyright 2015 Confluent Inc.
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

from muckrake.services.performance.performance import PerformanceService


class HadoopPerformanceService(PerformanceService):
    """
    This is a simple MapReduce job that makes sure that Hadoop is setup correctly
    """
    def __init__(self, context, num_nodes, hadoop, settings={}):
        super(HadoopPerformanceService, self).__init__(context, num_nodes)
        self.hadoop = hadoop
        self.settings = settings

        self.args = {
            'hadoop_path': self.hadoop.hadoop_home,
            'hadoop_bin_dir_name': self.hadoop.hadoop_bin_dir,
            'hadoop_example_jar': self.hadoop.hadoop_example_jar,
            'hadoop_conf_dir': '/mnt'
        }

    def _worker(self, idx, node):
        args = self.args.copy()
        self.hadoop.distribute_hdfs_confs(node)
        self.hadoop.distribute_mr_confs(node)

        cmd = "HADOOP_USER_CLASSPATH_FIRST=true HADOOP_CLASSPATH=%(hadoop_example_jar)s " \
              "HADOOP_CONF_DIR=%(hadoop_conf_dir)s %(hadoop_path)s/%(hadoop_bin_dir_name)s/hadoop jar " \
              "%(hadoop_example_jar)s pi " \
              "2 10 " % args
        for key, value in self.settings.items():
            cmd += " %s=%s" % (str(key), str(value))

        self.logger.info("Hadoop performance %d command: %s", idx, cmd)
        for line in node.account.ssh_capture(cmd):
            self.logger.info("Hadoop performance %d: %s", idx, line.strip())

    def clean_node(self, node):
        self.logger.debug("Cleaning %s on node %d on %s", self.__class__.__name__, self.idx(node), node.account.hostname)
        files = ['/mnt/capacity-scheduler.xml', '/mnt/core-site.xml', '/mnt/hadoop-env.sh', '/mnt/hadoop-metrics.properties', '/mnt/hdfs-site.xml', '/mnt/mapred-site.xml', '/mnt/yarn-env.sh', '/mnt/yarn-site.xml']
        cmd = "rm -rf %s" % " ".join(files)
        node.account.ssh(cmd, allow_fail=True)
