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
from muckrake.services.kafka_rest import KafkaRestService
from muckrake.services.schema_registry import SchemaRegistryService
from muckrake.services.register_schemas import RegisterSchemasService


class EverythingRunsTest(Test):
    """ Sanity check to ensure that various core services all run.
    """
    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(EverythingRunsTest, self).__init__(test_context=test_context)

        self.zk = ZookeeperService(test_context, num_nodes=2)
        self.kafka = KafkaService(test_context, 1, self.zk)
        self.schema_registry = SchemaRegistryService(test_context, 1, self.zk, self.kafka)
        self.rest_proxy = KafkaRestService(test_context, 1, self.zk, self.kafka, self.schema_registry)
        self.register_driver = RegisterSchemasService(
            test_context, 1, self.schema_registry,
            retry_wait_sec=.02, num_tries=5, max_time_seconds=10, max_schemas=50)

    def run(self):
        self.zk.start()
        self.kafka.start()
        self.schema_registry.start()
        self.rest_proxy.start()

        self.register_driver.start()
        self.register_driver.wait()  # block until register_driver finishes

