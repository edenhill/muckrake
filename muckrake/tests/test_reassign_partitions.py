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

from test import KafkaTest

import re
import random
import time


def parse_describe_topic(topic_description):
    """Parse output of kafka-topics.sh --describe, which is a string of form
    PartitionCount:2\tReplicationFactor:2\tConfigs:
        Topic: test_topic\ttPartition: 0\tLeader: 3\tReplicas: 3,1\tIsr: 3,1
        Topic: test_topic\tPartition: 1\tLeader: 1\tReplicas: 1,2\tIsr: 1,2


    into a dictionary structure appropriate for use with reassign-partitions tool:
    {
         "partitions": [
             {"topic": "test_topic", "partition": 0, "replicas": [3, 1]},
             {"topic": "test_topic", "partition": 1, "replicas": [1, 2]}
         ]
    }
    """

    lines = map(lambda x: x.strip(), topic_description.split("\n"))
    partitions = []
    for line in lines:
        m = re.match(".*Leader:.*", line)
        if m is None:
            continue

        fields = line.split("\t")
        # ["Partition: 4", "Leader: 0"] -> ["4", "0"]
        fields = map(lambda x: x.split(" ")[1], fields)
        partitions.append(
            {"topic": fields[0],
             "partition": int(fields[1]),
             "replicas": map(int, fields[3].split(','))})
    return {"partitions": partitions}


class TestReassignPartitions(KafkaTest):
    """
    Begin registering schemas; part way through, cleanly kill the master.
    """
    def __init__(self, test_context):
        super(TestReassignPartitions, self).__init__(test_context, num_zk=1, num_brokers=3,
                                                     topics={"test_topic": {
                                                                "partitions": 2,
                                                                "replication-factor": 2}
                                                            })
        self.num_partitions = 2
        self.timeout_sec = 60

    def setUp(self):
        super(TestReassignPartitions, self).setUp()

    def tearDown(self):
        super(TestReassignPartitions, self).tearDown()

    def run(self):

        partition_info = parse_describe_topic(self.kafka.describe_topic("test_topic"))
        self.logger.debug("Partitions before reassignment:" + str(partition_info))

        # jumble partition assignment in dictionary
        seed = random.randint(0, 2 ** 31 - 1)
        self.logger.debug("Jumble partition assignment with seed " + str(seed))
        random.seed(seed)
        # The list may still be in order, but that's ok
        shuffled_list = range(0, self.num_partitions)
        random.shuffle(shuffled_list)

        for i in range(0, self.num_partitions):
            partition_info["partitions"][i]["partition"] = shuffled_list[i]
        self.logger.debug("Jumbled partitions: " + str(partition_info))

        # send reassign partitions command
        self.kafka.execute_reassign_partitions(partition_info)

        # Wait until finished or timeout
        current_time = time.time()
        stop_time = current_time + self.timeout_sec
        success = False
        while current_time < stop_time:
            time.sleep(1)
            if self.kafka.verify_reassign_partitions(partition_info):
                success = True
                break
            current_time = time.time()
        assert success