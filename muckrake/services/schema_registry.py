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

import json
import re
import time
import urllib2

SCHEMA_REGISTRY_V1_JSON = "application/vnd.schemaregistry.v1+json"
SCHEMA_REGISTRY_V1_JSON_WEIGHTED = SCHEMA_REGISTRY_V1_JSON

# These are defaults that track the most recent API version. These should always be specified
# anywhere the latest version is produced/consumed.
SCHEMA_REGISTRY_MOST_SPECIFIC_DEFAULT = SCHEMA_REGISTRY_V1_JSON
SCHEMA_REGISTRY_DEFAULT_JSON = "application/vnd.schemaregistry+json"
SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED = SCHEMA_REGISTRY_DEFAULT_JSON + "; qs=0.9"
JSON = "application/json"
JSON_WEIGHTED = JSON + "; qs=0.5"

PREFERRED_RESPONSE_TYPES = [SCHEMA_REGISTRY_V1_JSON, SCHEMA_REGISTRY_DEFAULT_JSON, JSON]

# This type is completely generic and carries no actual information about the type of data, but
# it is the default for request entities if no content type is specified. Well behaving users
# of the API will always specify the content type, but ad hoc use may omit it. We treat this as
# JSON since that's all we currently support.
GENERIC_REQUEST = "application/octet-stream"

# Minimum header data necessary for using the Schema Registry REST api.
SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES = {"Content-Type": SCHEMA_REGISTRY_V1_JSON_WEIGHTED, "Accept": "*/*"}


class SchemaRegistryService(Service):

    logs = {
        "schema_registry_log": {
            "path": "/mnt/schema-registry.log",
            "collect_default": True
        }
    }

    def __init__(self, context, num_nodes, zk, kafka):
        """
        :type context
        :type zk: ZookeeperService
        :type kafka: muckrake.services.kafka.KafkaService
        """
        super(SchemaRegistryService, self).__init__(context, num_nodes)
        self.zk = zk
        self.kafka = kafka
        self.port = 8081

    def start_node(self, node):
        self.logger.info("Starting Schema Registry node %d on %s", self.idx(node), node.account.hostname)
        template = open('templates/schema-registry.properties').read()
        template_params = {
            'kafkastore_topic': '_schemas',
            'kafkastore_url': self.zk.connect_setting(),
            'rest_port': self.port
        }

        config = template % template_params
        if config is None:
            template = open('templates/schema-registry.properties').read()
            template_params = {
                'kafkastore_topic': '_schemas',
                'kafkastore_url': self.zk.connect_setting(),
                'rest_port': self.port
            }
            config = template % template_params

        node.account.create_file("/mnt/schema-registry.properties", config)
        cmd = "/opt/schema-registry/bin/schema-registry-start /mnt/schema-registry.properties " \
            + "1>> /mnt/schema-registry.log 2>> /mnt/schema-registry.log &"

        self.logger.debug("Attempting to start node with command: " + cmd)
        node.account.ssh(cmd)

        # Wait for the server to become live
        node.account.wait_for_http_service(self.port, headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)

    def stop_node(self, node, clean_shutdown=True, allow_fail=True):
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, self.idx(node), node.account.hostname))
        node.account.kill_process("schema-registry", clean_shutdown, allow_fail)

    def clean_node(self, node):
        self.logger.info("Cleaning %s node %d on %s" % (type(self).__name__, self.idx(node), node.account.hostname))
        node.account.ssh("rm -rf /mnt/schema-registry.properties /mnt/schema-registry.log")

    def restart_node(self, node, wait_sec=0, clean_shutdown=True):
        self.stop_node(node, clean_shutdown, allow_fail=True)
        time.sleep(wait_sec)
        self.start_node(node)

    def get_master_node(self):
        node = self.nodes[0]

        cmd = "/opt/kafka/bin/kafka-run-class.sh kafka.tools.ZooKeeperMainWrapper -server %s get /schema_registry/schema_registry_master" \
              % self.zk.connect_setting()

        host = None
        port_str = None
        self.logger.debug("Querying zookeeper to find current schema registry master: \n%s" % cmd)
        for line in node.account.ssh_capture(cmd):
            match = re.match("^{\"host\":\"(.*)\",\"port\":(\d+),", line)
            if match is not None:
                groups = match.groups()
                host = groups[0]
                port_str = groups[1]
                break

        if host is None:
            raise Exception("Could not find schema registry master.")

        base_url = "%s:%s" % (host, port_str)
        self.logger.debug("schema registry master is %s" % base_url)

        # Return the node with this base_url
        for idx, node in enumerate(self.nodes, 1):
            if self.url(idx).find(base_url) >= 0:
                return self.get_node(idx)

    def url(self, idx=1):
        return "http://" + self.get_node(idx).account.externally_routable_ip + ":" + str(self.port)


class RequestData(object):
    """
    Interface for classes wrapping data that goes in the body of an http request.
    """
    def to_json(self):
        raise NotImplementedError("Subclasses should implement to_json")


class RegisterSchemaRequest(RequestData):
    def __init__(self, schema_string):
        self.schema = schema_string

    def to_json(self):
        return json.dumps({"schema": self.schema})


class Compatibility(object):
    NONE = "NONE"
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"


class ConfigUpdateRequest(RequestData):
    def __init__(self, compatibility):
        # "NONE", "BACKWARD", "FORWARD", or "FULL"
        self.compatibility = compatibility.upper()

    def to_json(self):
        return json.dumps({"compatibility": self.compatibility.upper()})


def http_request(url, method, data="", headers=None):
    if url[0:7].lower() != "http://":
        url = "http://%s" % url

    req = urllib2.Request(url, data, headers)
    req.get_method = lambda: method
    return urllib2.urlopen(req)


def make_schema_string(num=None):
    if num is not None and num >= 0:
        field_name = "f%d" % num
    else:
        field_name = "f"

    schema_str = json.dumps({
        'type': 'record',
        'name': 'myrecord',
        'fields': [
            {
                'type': 'string',
                'name': field_name
            }
        ]
    })

    return schema_str


def ping_registry(base_url):
    resp = http_request(base_url, "GET", headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)
    return resp.getcode()


def register_schema(base_url, schema_string, subject):
    """
    return id of registered schema, or return -1 to indicate that the request was not successful.
    """

    request_data = RegisterSchemaRequest(schema_string).to_json()
    url = base_url + "/subjects/%s/versions" % subject
    resp = http_request(url, "POST", request_data, SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)

    data = json.loads(resp.read())
    return int(data["id"])


def update_config(base_url, compatibility, subject=None):
    if subject is None:
        url = "%s/config" % base_url
    else:
        url = "%s/config/%s" % (base_url, subject)

    request_data = ConfigUpdateRequest(compatibility).to_json()
    http_request(url, "PUT", request_data, SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)


def get_config(base_url, subject=None):
    if subject is None:
        url = "%s/config" % base_url
    else:
        url = "%s/config/%s" % (base_url, subject)
    resp = http_request(url, "GET", "", SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)

    # Return the config json directly without extracting data
    return resp.read()


def get_all_subjects(base_url):
    url = "%s/subjects" % base_url
    resp = http_request(url, "GET", headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)
    data = resp.read()

    return json.loads(data)


def get_by_schema(base_url, schema, subject):
    url = "%s/subjects/%s" % (base_url, subject)

    request_data = RegisterSchemaRequest(schema).to_json()
    resp = http_request(url, "POST", data=request_data, headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)

    return json.loads(resp.read())


def get_all_versions(base_url, subject):
    url = "%s/subjects/%s/versions" % (base_url, subject)
    resp = http_request(url, "GET", headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)

    return json.loads(resp.read())


def get_schema_by_version(base_url, subject, version):
    url = "%s/subjects/%s/versions/%d" % (base_url, subject, version)
    resp = http_request(url, "GET", headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)

    return json.loads(resp.read())


def get_schema_by_id(base_url, id):
    url = "%s/schemas/ids/%d" % (base_url, id)

    resp = http_request(url, "GET", headers=SCHEMA_REGISTRY_DEFAULT_REQUEST_PROPERTIES)
    return json.loads(resp.read())
