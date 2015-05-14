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

from .test import SchemaRegistryTest
from muckrake.services.performance.schema_registry import SchemaRegistryPerformanceService


class SchemaRegistryBenchmark(SchemaRegistryTest):
    def __init__(self, test_context):
        super(SchemaRegistryBenchmark, self).__init__(test_context, num_zk=1, num_brokers=3, num_schema_registry=1)

        self.num_schema_registry = 1
        self.subject = "testSubject"
        self.num_schemas = 10000
        self.schemas_per_sec = 1000

        self.schema_registry_perf = SchemaRegistryPerformanceService(
            test_context, self.num_schema_registry,
            self.schema_registry, self.subject, self.num_schemas, self.schemas_per_sec, settings={}
        )

    def run(self):
        self.logger.info("Running SchemaRegistryBenchmark: registering %d schemas on %d schema registry node." %
                         (self.num_schemas, self.num_schema_registry))
        self.schema_registry_perf.run()
        self.logger.info("Schema Registry performance: %f per sec, %f ms",
                         self.schema_registry_perf.results[0]['records_per_sec'],
                         self.schema_registry_perf.results[0]['latency_99th_ms'])
