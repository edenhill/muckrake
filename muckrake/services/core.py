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

import time, re, json
from .schema_registry_utils import SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES
from .kafka_rest_utils import KAFKA_REST_DEFAULT_REQUEST_PROPERTIES
import abc


class ZookeeperService(Service):
    def __init__(self, service_context):
        """
        :type service_context ducktape.services.service.ServiceContext
        """
        super(ZookeeperService, self).__init__(service_context)
        self.logs = {"zk_log": "/mnt/zk.log"}

    def start(self):
        super(ZookeeperService, self).start()
        config = """
dataDir=/mnt/zookeeper
clientPort=2181
maxClientCnxns=0
initLimit=5
syncLimit=2
quorumListenOnAllIPs=true
"""
        for idx, node in enumerate(self.nodes, 1):
            template_params = { 'idx': idx, 'host': node.account.hostname }
            config += "server.%(idx)d=%(host)s:2888:3888\n" % template_params

        for idx, node in enumerate(self.nodes, 1):
            self.logger.info("Starting ZK node %d on %s", idx, node.account.hostname)
            self._stop_and_clean(node, allow_fail=True)
            node.account.ssh("mkdir -p /mnt/zookeeper")
            node.account.ssh("echo %d > /mnt/zookeeper/myid" % idx)
            node.account.create_file("/mnt/zookeeper.properties", config)
            node.account.ssh(
                "/opt/kafka/bin/zookeeper-server-start.sh /mnt/zookeeper.properties 1>> %(zk_log)s 2>> %(zk_log)s &"
                % self.logs)
            time.sleep(5)  # give it some time to start

    def stop_node(self, node, allow_fail=True):
        # This uses Kafka-REST's stop service script because it's better behaved
        # (knows how to wait) and sends SIGTERM instead of
        # zookeeper-stop-server.sh's SIGINT. We don't actually care about clean
        # shutdown here, so it's ok to use the bigger hammer
        idx = self.idx(node)
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        node.account.ssh("/opt/kafka-rest/bin/kafka-rest-stop-service zookeeper", allow_fail=allow_fail)

    def clean_node(self, node, allow_fail=True):
        node.account.ssh("rm -rf /mnt/zookeeper /mnt/zookeeper.properties /mnt/zk.log", allow_fail=allow_fail)

    def stop(self):
        """If the service left any running processes or data, clean them up."""
        for idx, node in enumerate(self.nodes, 1):
            self.stop_node(node, allow_fail=False)
            self.clean_node(node)
            node.free()

    def _stop_and_clean(self, node, allow_fail=False):
        self.stop_node(node, allow_fail)
        self.clean_node(node, allow_fail)

    def connect_setting(self):
        return ','.join([node.account.hostname + ':2181' for node in self.nodes])


class KafkaRestService(Service):
    def __init__(self, service_context, zk, kafka, schema_registry=None):
        """
        :type service_context ducktape.services.service.ServiceContext
        :type zk: ZookeeperService
        :type kafka: muckrake.services.kafka_service.KafkaService
        :type schema_registry: SchemaRegistryService
        """
        super(KafkaRestService, self).__init__(service_context)
        self.zk = zk
        self.kafka = kafka
        self.schema_registry = schema_registry
        self.port = 8082

    def start(self):
        super(KafkaRestService, self).start()
        template = open('templates/rest.properties').read()
        zk_connect = self.zk.connect_setting()
        bootstrapServers = self.kafka.bootstrap_servers()
        for idx, node in enumerate(self.nodes, 1):
            self.logger.info("Starting REST node %d on %s", idx, node.account.hostname)
            self._stop_and_clean(node, allow_fail=True)
            template_params = {
                'id': idx,
                'port': self.port,
                'zk_connect': zk_connect,
                'bootstrap_servers': bootstrapServers,
                'schema_registry_url': None
            }

            if self.schema_registry is not None:
                template_params.update({'schema_registry_url': self.schema_registry.url()})

            self.logger.info("Schema registry url for Kafka rest proxy is %s", template_params['schema_registry_url'])
            config = template % template_params
            node.account.create_file("/mnt/rest.properties", config)
            node.account.ssh("/opt/kafka-rest/bin/kafka-rest-start /mnt/rest.properties 1>> /mnt/rest.log 2>> /mnt/rest.log &")

            node.account.wait_for_http_service(self.port, headers=KAFKA_REST_DEFAULT_REQUEST_PROPERTIES)

    def stop(self):
        for idx, node in enumerate(self.nodes, 1):
            self.logger.info("Stopping REST node %d on %s", idx, node.account.hostname)
            self._stop_and_clean(node)
            node.free()

    def _stop_and_clean(self, node, allow_fail=False):
        node.account.ssh("/opt/kafka-rest/bin/kafka-rest-stop", allow_fail=allow_fail)
        node.account.ssh("rm -rf /mnt/rest.properties /mnt/rest.log")

    def url(self, idx=1):
        return "http://" + self.get_node(idx).account.hostname + ":" + str(self.port)


class SchemaRegistryService(Service):
    def __init__(self, service_context, zk, kafka):
        """
        :type service_context ducktape.services.service.ServiceContext
        :type zk: ZookeeperService
        :type kafka: muckrake.services.kafka_service.KafkaService
        """
        super(SchemaRegistryService, self).__init__(service_context)
        self.zk = zk
        self.kafka = kafka
        self.port = 8081

    def start(self):
        super(SchemaRegistryService, self).start()

        template = open('templates/schema-registry.properties').read()
        template_params = {
            'kafkastore_topic': '_schemas',
            'kafkastore_url': self.zk.connect_setting(),
            'rest_port': self.port
        }
        config = template % template_params

        for idx, node in enumerate(self.nodes, 1):
            self.logger.info("Starting Schema Registry node %d on %s", idx, node.account.hostname)
            self._stop_and_clean(node, allow_fail=True)
            self.start_node(node, config)

            # Wait for the server to become live
            node.account.wait_for_http_service(self.port, headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)

    def stop(self):
        """If the service left any running processes or data, clean them up."""
        for idx, node in enumerate(self.nodes, 1):
            self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
            self._stop_and_clean(node, True)
            node.free()

    def _stop_and_clean(self, node, allow_fail=False):
        node.account.ssh("/opt/schema-registry/bin/schema-registry-stop", allow_fail=allow_fail)
        node.account.ssh("rm -rf /mnt/schema-registry.properties /mnt/schema-registry.log")

    def stop_node(self, node, clean_shutdown=True, allow_fail=True):
        node.account.kill_process("schema-registry", clean_shutdown, allow_fail)

    def start_node(self, node, config=None):
        if config is None:
            template = open('templates/schema-registry.properties').read()
            template_params = {
                'kafkastore_topic': '_schemas',
                'kafkastore_url': self.zk.connect_setting(),
                'rest_port': self.port
            }
            config = template % template_params

        node.account.create_file("/mnt/schema-registry.properties", config)
        cmd = "/opt/schema-registry/bin/schema-registry-start /mnt/schema-registry.properties " \
            + "1>> /mnt/schema-registry.log 2>> /mnt/schema-registry.log &"

        node.account.ssh(cmd)

    def restart_node(self, node, wait_sec=0, clean_shutdown=True):
        self.stop_node(node, clean_shutdown, allow_fail=True)
        time.sleep(wait_sec)
        self.start_node(node)

    def get_master_node(self):
        node = self.nodes[0]

        cmd = "/opt/kafka/bin/kafka-run-class.sh kafka.tools.ZooKeeperMainWrapper -server %s get /schema_registry/schema_registry_master" \
              % self.zk.connect_setting()

        host = None
        port_str = None
        self.logger.debug("Querying zookeeper to find current schema registry master: \n%s" % cmd)
        for line in node.account.ssh_capture(cmd):
            match = re.match("^{\"host\":\"(.*)\",\"port\":(\d+),", line)
            if match is not None:
                groups = match.groups()
                host = groups[0]
                port_str = groups[1]
                break

        if host is None:
            raise Exception("Could not find schema registry master.")

        base_url = "%s:%s" % (host, port_str)
        self.logger.debug("schema registry master is %s" % base_url)

        # Return the node with this base_url
        for idx, node in enumerate(self.nodes, 1):
            if self.url(idx).find(base_url) >= 0:
                return self.get_node(idx)

    def url(self, idx=1):
        return "http://" + self.get_node(idx).account.hostname + ":" + str(self.port)


def create_hadoop_service(service_context, hadoop_distro, hadoop_version):
    if hadoop_distro == 'cdh':
        hadoop_home = '/opt/hadoop-cdh/'
        if hadoop_version == 1:
            return CDHV1Service(service_context, hadoop_home)
        else:
            return CDHV2Service(service_context, hadoop_home)
    else:
        hadoop_home = '/usr/hdp/current/hadoop-hdfs-namenode/../hadoop/'
        return HDPService(service_context, hadoop_home)


class HDFSService(Service):
    def __init__(self, service_context, hadoop_home, hadoop_distro):
        """
        :type service_context ducktape.services.service.ServiceContext
        :type hadoop_home: str
        :type hadoop_distro: str
        """
        super(HDFSService, self).__init__(service_context)
        self.master_host = None
        self.slaves = []
        self.hadoop_home = hadoop_home
        self.hadoop_distro = hadoop_distro
        self.hadoop_bin_dir = 'bin'
        self.hadoop_example_jar = None

    def start(self):
        super(HDFSService, self).start()

        for idx, node in enumerate(self.nodes, 1):
            if idx == 1:
                self.master_host = node.account.hostname

            self.create_hdfs_dirs(node)
            self.distribute_hdfs_confs(node)
            self.logger.info("Stopping HDFS on %s", node.account.hostname)
            self._stop_and_clean_internal(node, allow_fail=True)

            if idx == 1:
                self.format_namenode(node)
                self.start_namenode(node)
            else:
                self.slaves.append(node.account.hostname)
                self.start_datanode(node)
            time.sleep(5)  # wait for start up

    def create_hdfs_dirs(self, node):
        self.logger.info("Creating hdfs directories on %s", node.account.hostname)
        node.account.ssh("mkdir -p /mnt/data")
        node.account.ssh("mkdir -p /mnt/name")
        node.account.ssh("mkdir -p /mnt/logs")

    def distribute_hdfs_confs(self, node):
        self.logger.info("Distributing hdfs confs to %s", node.account.hostname)

        template_path = 'templates/' + self.hadoop_distro + '/'

        hadoop_env_template = open(template_path + 'hadoop-env.sh').read()
        hadoop_env_params = {'java_home': '/usr/lib/jvm/java-6-oracle'}
        hadoop_env = hadoop_env_template % hadoop_env_params

        core_site_template = open(template_path + 'core-site.xml').read()
        core_site_params = {
            'fs_default_name': "hdfs://" + self.master_host + ":9000"
        }
        core_site = core_site_template % core_site_params

        hdfs_site_template = open(template_path + 'hdfs-site.xml').read()
        hdfs_site_params = {
            'dfs_replication': 1,
            'dfs_name_dir': '/mnt/name',
            'dfs_data_dir': '/mnt/data'
        }
        hdfs_site = hdfs_site_template % hdfs_site_params

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

    def stop(self):
        for idx, node in enumerate(self.nodes, 1):
            self._stop_and_clean_internal(node)
            node.free()

    def _stop_and_clean_internal(self, node, allow_fail=False):
        self.logger.info("Force cleaning HDFS processes on %s", node.account.hostname)
        pids = list(node.account.ssh_capture("ps ax | grep java | grep -v grep | awk '{print $1}'"))
        for pid in pids:
            node.account.ssh("kill -9 " + pid)
        time.sleep(5)  # the stop script doesn't wait
        self.logger.info("Removing HDFS directories on %s", node.account.hostname)
        node.account.ssh("rm -rf /mnt/data/ /mnt/name/ /mnt/logs")


class CDHV1Service(HDFSService):
    def __init__(self, service_context, hadoop_home):
        """
        :type service_context ducktape.services.service.ServiceContext
        :type hadoop_home: str
        """
        super(CDHV1Service, self).__init__(service_context, hadoop_home, 'cdh')
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

    def distribute_mr_confs(self, node):
        self.logger.info("Distributing MR1 confs to %s", node.account.hostname)

        template_path = 'templates/' + self.hadoop_distro + '/'

        mapred_site_template = open(template_path + 'mapred-site.xml').read()

        mapred_site_params = {
            'mapred_job_tracker': self.master_host + ":54311",
            'mapreduce_jobhistory_address': self.master_host + ":10020"
        }

        mapred_site = mapred_site_template % mapred_site_params
        node.account.create_file("/mnt/mapred-site.xml", mapred_site)

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
    def __init__(self, service_context, hadoop_home):
        """
        :type service_context ducktape.services.service.ServiceContext
        :type hadoop_home: str
        """
        super(CDHV2Service, self).__init__(service_context, hadoop_home, 'cdh')
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
    
    def distribute_mr_confs(self, node):
        self.logger.info("Distributing YARN confs to %s", node.account.hostname)

        template_path = 'templates/' + self.hadoop_distro + '/'

        mapred_site_template = open(template_path + 'mapred2-site.xml').read()
        mapred_site_params = {
            'mapreduce_jobhistory_address': self.master_host + ":10020"
        }
        mapred_site = mapred_site_template % mapred_site_params

        yarn_env_template = open(template_path + 'yarn-env.sh').read()
        yarn_env_params = {
            'java_home': '/usr/lib/jvm/java-6-oracle'
        }
        yarn_env = yarn_env_template % yarn_env_params

        yarn_site_template = open(template_path + 'yarn-site.xml').read()
        yarn_site_params = {
            'yarn_resourcemanager_hostname': self.master_host
        }
        yarn_site = yarn_site_template % yarn_site_params

        node.account.create_file("/mnt/mapred-site.xml", mapred_site)
        node.account.create_file("/mnt/yarn-env.sh", yarn_env)
        node.account.create_file("/mnt/yarn-site.xml", yarn_site)
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
    def __init__(self, service_context, hadoop_home):
        """
        :type service_context ducktape.services.service.ServiceContext
        :type hadoop_home: str
        """
        super(HDPService, self).__init__(service_context, hadoop_home, 'hdp')
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

        template_path = 'templates/' + self.hadoop_distro + '/'

        yarn_env_template = open(template_path + 'yarn-env.sh').read()
        yarn_env_params = {
            'java_home': '/usr/lib/jvm/java-6-oracle'
        }
        yarn_env = yarn_env_template % yarn_env_params

        mapred_site_template = open(template_path + 'mapred-site.xml').read()
        mapred_site_params = {
            'jobhistory_host': self.master_host
        }
        mapred_site = mapred_site_template % mapred_site_params

        yarn_site_template = open(template_path + 'yarn-site.xml').read()
        yarn_site_params = {
            'yarn_resourcemanager_hostname': self.master_host
        }
        yarn_site = yarn_site_template % yarn_site_params

        node.account.create_file("/mnt/mapred-site.xml", mapred_site)
        node.account.create_file("/mnt/yarn-site.xml", yarn_site)
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
