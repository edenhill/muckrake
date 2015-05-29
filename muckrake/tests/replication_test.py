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

from muckrake.services.zookeeper import ZookeeperService
from muckrake.services.kafka import KafkaService
from muckrake.services.verbose_producer import VerboseProducerService
from muckrake.services.console_consumer import ConsoleConsumerService

import time


class ReplicationTest(Test):
    """ Simple replication test."""
    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ReplicationTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=2)
        self.kafka = KafkaService(test_context, num_nodes=3, zk=self.zk, topics={self.topic: {
                                                                    "partitions": 3,
                                                                    "replication-factor": 3,
                                                                    "min.insync.replicas": 2}
                                                                })
        self.msgs = 100000
        self.timeout_sec = 600

    def run_with_failure(self, failure):
        """This is the top-level test template."""
        self.producer = VerboseProducerService(self.test_context, 1, self.kafka, self.topic, self.msgs)
        self.consumer = ConsoleConsumerService(self.test_context, 1, self.kafka, self.topic, consumer_timeout_ms=3000)

        self.zk.start()
        self.kafka.start()

        # Produce with failures
        self.producer.start()
        time.sleep(.25)
        failure()
        self.producer.wait()

        self.not_acked = []
        self.acked = []
        while not self.producer.not_acked_values.empty():
            self.not_acked.append(self.producer.not_acked_values.get())
        while not self.producer.acked_values.empty():
            self.acked.append(self.producer.acked_values.get())

        self.logger.info("num not acked: %d" % len(self.not_acked))
        self.logger.info("num acked:     %d" % len(self.acked))

        # Consume messages and validate
        self.consumer.start()
        self.consumer.wait()
        self.consumed = [int(msg) for msg in self.consumer.messages_consumed[1]]
        self.logger.info("num consumed:  %d" % len(self.consumed))

        self.validate()

    def clean_shutdown(self):
        """Discover leader node for our topic and shut it down cleanly."""
        kafka_leader = self.kafka.get_leader_node(self.topic, partition=0)
        self.kafka.stop_node(kafka_leader, clean_shutdown=True)

    def hard_shutdown(self):
        """Discover leader node for our topic and shut it down with a hard kill."""
        kafka_leader = self.kafka.get_leader_node(self.topic, partition=0)
        self.kafka.stop_node(kafka_leader, clean_shutdown=False)

    def clean_bounce(self):
        """Chase the leader and restart it cleanly."""
        for i in range(5):
            prev_leader_node = self.kafka.get_leader_node(topic=self.topic, partition=0)
            self.kafka.restart_node(prev_leader_node, wait_sec=5, clean_shutdown=True)

            # Wait long enough for previous leader to probably be awake again
            time.sleep(6)

    def hard_bounce(self):
        """Chase the leader and restart it cleanly."""
        for i in range(5):
            prev_leader_node = self.kafka.get_leader_node(topic=self.topic, partition=0)
            self.kafka.restart_node(prev_leader_node, wait_sec=5, clean_shutdown=False)

            # Wait long enough for previous leader to probably be awake again
            time.sleep(6)

    def validate(self):
        """Check that produced messages were consumed."""

        num_acked = len(self.acked)
        num_not_acked = len(self.not_acked)

        success = True
        msg = ""
        if num_acked + num_not_acked != self.msgs:
            success = False
            msg += "acked + not_acked != total attempted: %d != %d" % (num_acked + num_not_acked, self.msgs) + "\n"

        if len(set(self.consumed)) != len(self.consumed):
            # There are duplicates. This is ok, so report it but don't fail the test
            msg += "There are duplicate messages in the log\n"

        if not set(self.consumed).issuperset(set(self.acked)):
            # Every acked message must appear in the logs. I.e. consumed messages must be superset of acked messages.
            acked_minus_consumed = set(self.acked) - set(self.consumed)
            success = False
            msg += "At least one acked message did not appear in the consumed messages. acked_minus_consumed: " + str(acked_minus_consumed)

        if not success:
            # Collect all the data logs if there was a failure
            self.mark_for_collect(self.kafka)

        assert success, msg

    def test_clean_shutdown(self):
        self.run_with_failure(self.clean_shutdown)

    def test_hard_shutdown(self):
        self.run_with_failure(self.hard_shutdown)

    def test_clean_bounce(self):
        self.msgs = 300000
        self.run_with_failure(self.clean_bounce)

    def test_hard_bounce(self):
        self.msgs = 300000
        self.run_with_failure(self.hard_bounce)



