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
from .test import CamusTest
from muckrake.services.performance import CamusPerformanceService


class CamusHadoopV1Test(CamusTest):
    """
    7 machines are required at minimum: Zookeeper 1, Kafka 1, Hadoop 2,
    SchemaRegistry 1,  KafkaRest 1,
    CamusPerformance 1
    """

    def __init__(self, test_context):
        super(CamusHadoopV1Test, self).__init__(
            test_context,
            num_zk=1,
            num_brokers=1,
            num_hadoop=2,
            num_schema_registry=1,
            num_rest=1,
            hadoop_distro='cdh',
            hadoop_version=1,
            topics={"testAvro": None})

        self.num_camus_perf = 1
        self.camus_perf = CamusPerformanceService(
            test_context, self.num_camus_perf,
            self.kafka, self.hadoop, self.schema_registry, self.rest, settings={})

    def run(self):
        self.logger.info("Running Camus example on Hadoop distribution %s, Hadoop version %d",
                         self.hadoop_distro, self.hadoop_version)
        self.camus_perf.run()


class CamusHadoopV2Test(CamusTest):
    """
    7 machines are required at minimum: Zookeeper 1, Kafka 1, Hadoop 2,
    SchemaRegistry 1,  KafkaRest 1,
    CamusPerformance 1
    """

    def __init__(self, test_context):
        super(CamusHadoopV2Test, self).__init__(
            test_context,
            num_zk=1,
            num_brokers=1,
            num_hadoop=2,
            num_schema_registry=1,
            num_rest=1,
            hadoop_distro='cdh',
            hadoop_version=2,
            topics={"testAvro": None})

        self.num_camus_perf = 1

        self.camus_perf = CamusPerformanceService(
            test_context, self.num_camus_perf,
            self.kafka, self.hadoop, self.schema_registry, self.rest, settings={})

    def run(self):
        self.logger.info("Running Camus example on Hadoop distribution %s, Hadoop version %d",
                         self.hadoop_distro, self.hadoop_version)
        self.camus_perf.run()


class CamusHDPTest(CamusTest):
    """
    7 machines are required at minimum: Zookeeper 1, Kafka 1, Hadoop 2,
    SchemaRegistry 1,  KafkaRest 1,
    CamusPerformance 1
    """

    def __init__(self, test_context):
        super(CamusHDPTest, self).__init__(
            test_context,
            num_zk=1,
            num_brokers=1,
            num_hadoop=2,
            num_schema_registry=1,
            num_rest=1,
            hadoop_distro='hdp',
            hadoop_version=2,
            topics={"testAvro": None})

        self.num_camus_perf = 1
        self.camus_perf = CamusPerformanceService(
            test_context, self.num_camus_perf,
            self.kafka, self.hadoop, self.schema_registry, self.rest, settings={})

    def run(self):
        self.logger.info("Running Camus example on Hadoop distribution %s, Hadoop version %d",
                         self.hadoop_distro, self.hadoop_version)
        self.camus_perf.run()
