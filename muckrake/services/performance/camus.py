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

from muckrake.services.performance.performance import PerformanceService

import json
import requests


class CamusPerformanceService(PerformanceService):
    def __init__(self, context, num_nodes, kafka, hadoop, schema_registry, rest, settings={}):
        super(CamusPerformanceService, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.hadoop = hadoop
        self.schema_registry = schema_registry
        self.rest = rest
        self.settings = settings

    def _worker(self, idx, node):
        camus_path = '/opt/camus/camus-example/'
        self.args = {
            'hadoop_path': self.hadoop.hadoop_home,
            'hadoop_bin_dir_name': self.hadoop.hadoop_bin_dir,
            'camus_path': camus_path,
            'camus_jar': 'confluent-camus-0.1.0-SNAPSHOT.jar',
            'camus_property': '/mnt/camus.properties',
            'camus_main': 'com.linkedin.camus.etl.kafka.CamusJob',
            'broker_list': self.kafka.bootstrap_servers(),
            'schema_registry_url': self.schema_registry.url(),
            'rest_url': self.rest.url(),
            'topic': 'testAvro'
        }
        args = self.args.copy()

        self.produce_avro(self.rest.url(idx=1, external=True) + '/topics/' + args['topic'])

        self.hadoop.distribute_hdfs_confs(node)
        self.hadoop.distribute_mr_confs(node)
        node.account.create_file(self.args['camus_property'], self.render('camus.properties', **self.args))

        cmd_template = "PATH=%(hadoop_path)s/%(hadoop_bin_dir_name)s:$PATH HADOOP_CONF_DIR=/mnt /opt/camus/bin/camus-run " \
                       "-D schema.registry.url=%(schema_registry_url)s -P %(camus_property)s " \
                       "-Dlog4j.configuration=file:%(camus_path)s/log4j.xml"
        cmd = cmd_template % args

        for key, value in self.settings.items():
            cmd += " %s=%s" % (str(key), str(value))

        self.logger.debug("Camus performance %d command: %s", idx, cmd)
        # last = None
        for line in node.account.ssh_capture(cmd):
            self.logger.info("Camus performance %d: %s", idx, line.strip())
            # last = line
        # Parse and save the last line's information
        # self.results[idx-1] = parse_performance_output(last)
        node.account.ssh("rm -rf /mnt/camus.properties")

    def clean_node(self, node):
        self.logger.debug("Cleaning %s on node %d on %s", self.__class__.__name__, self.idx(node), node.account.hostname)
        files = ['/mnt/capacity-scheduler.xml', '/mnt/core-site.xml', '/mnt/hadoop-env.sh', '/mnt/hadoop-metrics.properties', '/mnt/hdfs-site.xml', '/mnt/mapred-site.xml', '/mnt/yarn-env.sh', '/mnt/yarn-site.xml', '/mnt/camus.properties']
        cmd = "rm -rf %s" % " ".join(files)
        node.account.ssh(cmd, allow_fail=True)

    def produce_avro(self, url):
        value_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {
                    "name": "name",
                    "type": "string"
                }
            ]
        }

        payload = {
            "value_schema": json.dumps(value_schema),
            "records": [
                {"value": {"name": "testUser"}},
                {"value": {"name": "testUser"}},
                {"value": {"name": "testUser"}},
                {"value": {"name": "testUser"}},
                {"value": {"name": "testUser"}},
                {"value": {"name": "testUser"}},
                {"value": {"name": "testUser"}},
                {"value": {"name": "testUser"}}
            ]
        }

        payload2 = {
            "records": [
                {"value":  "null"}
            ]
        }

        value_schema2 = {
            "type": "record",
            "name": "User",
            "fields": [
                {
                    "name": "name",
                    "type": "string"
                },
                {
                    "name": "id",
                    "type": "int",
                    "default": 0
                }
            ]
        }

        payload3 = {
            "value_schema": json.dumps(value_schema2),
            "records": [
                {"value": {"name": "testUser", "id": 1}},
                {"value": {"name": "testUser", "id": 1}},
            ]
        }

        self.logger.info("Produce avro data with schema %s", json.dumps(value_schema))
        avro_headers = {'content-type': 'application/vnd.kafka.avro.v1+json'}

        response = requests.post(url, data=json.dumps(payload), headers=avro_headers)
        self.logger.info("Response %s", response.status_code)
        self.logger.debug("Response data %s", response.text)

        self.logger.info("Produce binary data")
        binary_headers = {'content-type': 'application/vnd.kafka.binary.v1+json'}
        response = requests.post(url, data=json.dumps(payload2), headers=binary_headers)
        self.logger.info("Response %s", response.status_code)
        self.logger.debug("Response data %s", response.text)

        self.logger.info("Produce avro data with schema %s", json.dumps(value_schema2))
        response = requests.post(url, data=json.dumps(payload3), headers=avro_headers)
        self.logger.info("Response %s", response.status_code)
        self.logger.debug("Response data %s", response.text)



