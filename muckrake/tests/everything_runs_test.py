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
from ducktape.services.service_registry import ServiceRegistry

from muckrake.services.zookeeper_service import ZookeeperService
from muckrake.services.kafka_service import KafkaService
from muckrake.services.core import KafkaRestService
from muckrake.services.core import SchemaRegistryService
from muckrake.services.register_schemas_service import RegisterSchemasService


class EverythingRunsTest(Test):
    """ Sanity check to ensure that various core services all run.
    """
    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(EverythingRunsTest, self).__init__(test_context=test_context)

        # register a bunch of services
        self.services = ServiceRegistry()
        self.services['zk'] = ZookeeperService(self.service_context(num_nodes=2))

        self.services['kafka'] = KafkaService(self.service_context(num_nodes=1), self.services['zk'])

        self.services['schema_registry'] = SchemaRegistryService(
            self.service_context(num_nodes=1),
            self.services['zk'], self.services['kafka'])

        self.services['resty_proxy'] = KafkaRestService(
            self.service_context(num_nodes=1),
            self.services['zk'], self.services['kafka'], self.services['schema_registry'])

        self.services['register_driver'] = RegisterSchemasService(
            self.service_context(num_nodes=1),
            self.services['schema_registry'],
            retry_wait_sec=.02, num_tries=5,
            max_time_seconds=10, max_schemas=50)

    def run(self):
        self.services['zk'].start()
        self.services['kafka'].start()
        self.services['schema_registry'].start()
        self.services['resty_proxy'].start()

        self.services['register_driver'].start()
        self.services['register_driver'].wait() # block until register_driver finishes

