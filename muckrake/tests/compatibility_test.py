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

from ducktape.tests.test import Test

from muckrake.tests.test import KafkaTest
from muckrake.services.performance.kafka import ProducerPerformanceService, ConsumerPerformanceService
from muckrake.services.zookeeper import ZookeeperService
from muckrake.services.kafka import KafkaService

import time


class EndToEndTest(Test):

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(EndToEndTest, self).__init__(test_context)
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, 3, self.zk, topics={'test': {'partitions': 6, 'replication-factor': 1},
        })

        self.msgs = 5000000
        self.msg_size_default = 100
        self.batch_size = 8*1024
        self.buffer_memory = 64*1024*1024

        self.producer_perf = ProducerPerformanceService(
            self.test_context, 1, self.kafka,
            topic="test", num_records=self.msgs, record_size=self.msg_size_default, throughput=-1,
            settings={'acks': 1, 'batch.size': self.batch_size, 'buffer.memory': self.buffer_memory}
        )

        self.consumer_perf = ConsumerPerformanceService(
            self.test_context, 1, self.kafka,
            topic="test", num_records=2 * self.msgs, throughput=-1, threads=1
        )

    def broken_test_0811_client(self):
        self.zk.start()
        self.kafka.update_version("0.8.2.1")
        self.kafka.start()

        client_version = "0.8.1.1"
        self.producer_perf.update_version(client_version)
        self.consumer_perf.update_version(client_version)

        self.producer_perf.run()
        self.consumer_perf.run()

        assert self.consumer_perf.results[0]['num_messages'] == self.msgs

    def test_upgrade(self):
        """Coarse-grained rolling upgrade test. Verify only the number of published messages, not the content."""
        self.zk.start()
        self.kafka.update_version("0.8.1.1")
        self.kafka.start()

        # produce a bunch of messages
        self.producer_perf.run()

        # do the upgrade
        for node in self.kafka.nodes:
            idx = self.kafka.idx(node)
            self.kafka.stop_node(node)
            time.sleep(5)
            self.kafka.version[idx] = "0.8.2.1"
            self.kafka.start_node(node)

        # Can we get back the same number of messages?
        self.consumer_perf.run()
        assert self.consumer_perf.results[0]['num_messages'] == self.msgs
