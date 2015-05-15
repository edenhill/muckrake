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

from ducktape.services.service import Service

import re
import time


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

    def start(self):
        super(ZookeeperService, self).start()
        self.wait_for_start()

    def start_node(self, node):
        idx = self.idx(node)
        self.logger.info("Starting ZK node %d on %s", idx, node.account.hostname)

        node.account.ssh("mkdir -p /mnt/zookeeper")
        node.account.ssh("echo %d > /mnt/zookeeper/myid" % idx)
        node.account.create_file("/mnt/zookeeper.properties", self.render('zookeeper.properties'))
        node.account.ssh(
            "/opt/kafka-0.8.2.1/bin/zookeeper-server-start.sh /mnt/zookeeper.properties 1>> %(path)s 2>> %(path)s &"
            % self.logs["zk_log"])

    def wait_for_start(self, timeout=5):
        node = self.nodes[0]
        stop_time = time.time() + timeout
        while time.time() < stop_time:
            cmd = "/opt/kafka-0.8.2.1/bin/kafka-run-class.sh kafka.tools.ZooKeeperMainWrapper -server %s:%d ls /" \
                  % (node.account.hostname, 2181)

            for line in node.account.ssh_capture(cmd, allow_fail=True):
                match = re.match("^\[(.*)\]$", line)
                if match is not None:
                    groups = match.groups()
                    znodes = [z.strip() for z in groups[0].split(",")]
                    if "zookeeper" in znodes:
                        self.logger.debug("zookeeper appears to be awake")
                        return
            time.sleep(.25)

        msg = "Timed out waiting for zookeeper service to start"
        self.logger.debug(msg)
        raise Exception(msg)



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
