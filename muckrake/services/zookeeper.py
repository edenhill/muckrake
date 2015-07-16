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

import subprocess
from ducktape.services.service import Service
from ducktape.utils import wait


class ZookeeperService(Service):

    logs = {
        "zk_log": {
            "path": "/mnt/zk.log",
            "collect_default": True}
    }

    def __init__(self, context, num_nodes):
        """
        :type context
        """
        super(ZookeeperService, self).__init__(context, num_nodes)

    def alive (self, node):
        """ Returns True if Zookeeper is running on 'node', else False """
        try:
            node.account.ssh("nc -vnz 127.0.0.1 2181")
            return True
        except subprocess.CalledProcessError:
            return False
        except:
            raise

    def start_node(self, node):
        idx = self.idx(node)
        self.logger.info("Starting ZK node %d on %s", idx, node.account.hostname)

        node.account.ssh("mkdir -p /mnt/zookeeper")
        node.account.ssh("echo %d > /mnt/zookeeper/myid" % idx)
        node.account.create_file("/mnt/zookeeper.properties", self.render('zookeeper.properties'))

        self.logger.debug('Starting zookeeper on %s' % node.account.hostname)
        node.account.ssh(
            "/opt/kafka/bin/zookeeper-server-start.sh /mnt/zookeeper.properties 1>> %(path)s 2>> %(path)s &"
            % self.logs["zk_log"])
        # Wait for node to start
        wait.until(10, self.alive, node)

    def stop_node(self, node, allow_fail=True):
        # This uses Kafka-REST's stop service script because it's better behaved
        # (knows how to wait) and sends SIGTERM instead of
        # zookeeper-stop-server.sh's SIGINT. We don't actually care about clean
        # shutdown here, so it's ok to use the bigger hammer
        idx = self.idx(node)
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        node.account.ssh("/opt/kafka-rest/bin/kafka-rest-stop-service zookeeper", allow_fail=allow_fail)

    def clean_node(self, node, allow_fail=True):
        self.logger.info("Cleaning ZK node %d on %s", self.idx(node), node.account.hostname)
        node.account.ssh("rm -rf /mnt/zookeeper /mnt/zookeeper.properties /mnt/zk.log", allow_fail=allow_fail)

    def connect_setting(self):
        return ','.join([node.account.hostname + ':2181' for node in self.nodes])
