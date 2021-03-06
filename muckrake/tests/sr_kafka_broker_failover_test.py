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

from .test import SchemaRegistryFailoverTest
import time


# Several tests to check that Schema Registry gracefully handles failure of the (kafka broker) leader
# of the _schemas topic.


class KafkaLeaderCleanFailover(SchemaRegistryFailoverTest):
    """
    Begin registering schemas; part way through, cleanly kill the leader node for "_schemas" topic.
    """
    def __init__(self, test_context):
        super(KafkaLeaderCleanFailover, self).__init__(
            test_context, num_zk=1, num_brokers=3, num_schema_registry=1, retry_wait_sec=.02, num_retries=300)

    def drive_failures(self):
        time.sleep(3)
        leader_node = self.kafka.leader(topic="_schemas", partition=0)
        self.kafka.stop_node(leader_node)


class KafkaLeaderHardFailover(SchemaRegistryFailoverTest):
    """
    Begin registering schemas; part way through, kill -9 the leader node for "_schemas" topic
    """
    def __init__(self, test_context):
        super(KafkaLeaderHardFailover, self).__init__(
            test_context, num_zk=1, num_brokers=3, num_schema_registry=1, retry_wait_sec=.1, num_retries=300)

    def drive_failures(self):
        time.sleep(3)
        leader_node = self.kafka.leader(topic="_schemas", partition=0)
        self.kafka.stop_node(leader_node, clean_shutdown=False)


class KafkaBrokerCleanBounce(SchemaRegistryFailoverTest):
    def __init__(self, test_context):
        super(KafkaBrokerCleanBounce, self).__init__(
            test_context, num_zk=1, num_brokers=3, num_schema_registry=1, retry_wait_sec=.02, num_retries=300)

    def drive_failures(self):
        # Bounce leader several times with some wait in-between
        for i in range(5):
            prev_leader_node = self.kafka.leader(topic="_schemas", partition=0)
            self.kafka.restart_node(prev_leader_node, wait_sec=5, clean_shutdown=False)

            # Wait long enough for previous leader to probably be awake again
            time.sleep(6)


class KafkaBrokerHardBounce(SchemaRegistryFailoverTest):
    def __init__(self, test_context):
        super(KafkaBrokerHardBounce, self).__init__(
            test_context, num_zk=1, num_brokers=3, num_schema_registry=1, retry_wait_sec=.3, num_retries=300)

    def drive_failures(self):
        # Bounce leader several times with some wait in-between
        for i in range(1):
            prev_leader_node = self.kafka.leader(topic="_schemas", partition=0)
            self.kafka.restart_node(prev_leader_node, wait_sec=7, clean_shutdown=False)

            # Wait long enough for previous leader to probably be awake again
            time.sleep(10)
