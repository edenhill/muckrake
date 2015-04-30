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

from ducktape.services.service import Service

KAFKA_V1_JSON = "application/vnd.kafka.v1+json"
KAFKA_V1_JSON_WEIGHTED = KAFKA_V1_JSON

# These are defaults that track the most recent API version. These should always be specified
# anywhere the latest version is produced/consumed.
KAFKA_MOST_SPECIFIC_DEFAULT = KAFKA_V1_JSON
KAFKA_DEFAULT_JSON = "application/vnd.kafka+json"
KAFKA_DEFAULT_JSON_WEIGHTED = KAFKA_DEFAULT_JSON + "; qs=0.9"
JSON = "application/json"
JSON_WEIGHTED = JSON + "; qs=0.5"

PREFERRED_RESPONSE_TYPES = [KAFKA_V1_JSON, KAFKA_DEFAULT_JSON, JSON]

# Minimal header for using the kafka rest api
KAFKA_REST_DEFAULT_REQUEST_PROPERTIES = {"Content-Type": KAFKA_V1_JSON_WEIGHTED, "Accept": "*/*"}


class KafkaRestService(Service):

    logs = {
        "rest_log": {
            "path": "/mnt/rest.log",
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, zk, kafka, schema_registry=None):
        """
        :type context
        :type zk: ZookeeperService
        :type kafka: muckrake.services.kafka.KafkaService
        :type schema_registry: SchemaRegistryService
        """
        super(KafkaRestService, self).__init__(context, num_nodes)
        self.zk = zk
        self.kafka = kafka
        self.schema_registry = schema_registry
        self.port = 8082

    def start_node(self, node):
        template = open('templates/rest.properties').read()
        zk_connect = self.zk.connect_setting()
        bootstrapServers = self.kafka.bootstrap_servers()
        idx = self.idx(node)

        self.logger.info("Starting REST node %d on %s", idx, node.account.hostname)
        template_params = {
            'id': idx,
            'port': self.port,
            'zk_connect': zk_connect,
            'bootstrap_servers': bootstrapServers,
            'schema_registry_url': None
        }

        if self.schema_registry is not None:
            template_params.update({'schema_registry_url': self.schema_registry.url()})

        self.logger.info("Schema registry url for Kafka rest proxy is %s", template_params['schema_registry_url'])
        config = template % template_params
        node.account.create_file("/mnt/rest.properties", config)
        node.account.ssh(
            "/opt/kafka-rest/bin/kafka-rest-start /mnt/rest.properties 1>> %(path)s 2>> %(path)s &" %
            self.logs["rest_log"])

        # Block until we get a response from the service
        node.account.wait_for_http_service(self.port, headers=KAFKA_REST_DEFAULT_REQUEST_PROPERTIES)

    def stop_node(self, node):
        self.logger.info("Stopping REST node %d on %s", self.idx(node), node.account.hostname)
        node.account.ssh("/opt/kafka-rest/bin/kafka-rest-stop", allow_fail=True)

    def clean_node(self, node):
        self.logger.info("Cleaning REST node %d on %s", self.idx(node), node.account.hostname)
        node.account.ssh("rm -rf /mnt/rest.properties %(path)s" % self.logs["rest_log"], allow_fail=True)

    def url(self, idx=1):
        return "http://" + self.get_node(idx).account.hostname + ":" + str(self.port)

