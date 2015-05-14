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

from muckrake.services.performance.performance import PerformanceService, parse_performance_output


class SchemaRegistryPerformanceService(PerformanceService):
    def __init__(self, context, num_nodes, schema_registry, subject, num_schemas, schemas_per_sec, settings={}):
        super(SchemaRegistryPerformanceService, self).__init__(context, num_nodes)
        self.schema_registry = schema_registry

        self.args = {
            'subject': subject,
            'num_schemas': num_schemas,
            'schemas_per_sec': schemas_per_sec
        }
        self.settings = settings

    def _worker(self, idx, node):
        args = self.args.copy()

        args.update({'schema_registry_url': self.schema_registry.url()})

        cmd = "/opt/schema-registry/bin/schema-registry-run-class io.confluent.kafka.schemaregistry.tools.SchemaRegistryPerformance "\
              "'%(schema_registry_url)s' %(subject)s %(num_schemas)d %(schemas_per_sec)d" % args
        for key, value in self.settings.items():
            cmd += " %s=%s" % (str(key), str(value))

        self.logger.debug("Schema Registry performance %d command: %s", idx, cmd)
        last = None
        for line in node.account.ssh_capture(cmd):
            self.logger.info("Schema Registry performance %d: %s", idx, line.strip())
            last = line
        # Parse and save the last line's information
        self.results[idx-1] = parse_performance_output(last)