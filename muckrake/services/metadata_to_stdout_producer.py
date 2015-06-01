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

from muckrake.services.background_thread import BackgroundThreadService

import json
from Queue import Queue


class MetadataToStdoutProducerService(BackgroundThreadService):

    logs = {
        "producer_log": {
            "path": "/mnt/producer.log",
            "collect_default": True}
    }

    def __init__(self, context, num_nodes, kafka, topic, num_messages):
        super(MetadataToStdoutProducerService, self).__init__(context, num_nodes)

        self.kafka = kafka
        self.args = {
            'topic': topic,
            'num_messages': num_messages
        }

        self.acked_values = Queue()
        self.not_acked_data = Queue()
        self.not_acked_values = Queue()

    def _worker(self, idx, node):
        args = self.args.copy()
        args.update({'bootstrap_servers': self.kafka.bootstrap_servers()})
        cmd = "/opt/kafka/bin/kafka-run-class.sh org.apache.kafka.clients.tools.MetadataToStdoutProducer --topic %(topic)s --broker-list %(bootstrap_servers)s --num-messages %(num_messages)s 2>> /mnt/producer.log | tee -a /mnt/producer.log &" % args

        self.logger.debug("Verbose producer %d command: %s" % (idx, cmd))

        for line in node.account.ssh_capture(cmd):
            line = line.strip()

            data = self.try_parse_json(line)
            if data is not None:
                self.logger.debug("MetadataToStdoutProducer: " + str(data))
                if "exception" in data.keys():
                    data["node"] = idx
                    self.not_acked_data.put(data, block=True)
                    self.not_acked_values.put(int(data["value"]), block=True)
                else:
                    self.acked_values.put(int(data["value"]), block=True)

    def stop_node(self, node):
        node.account.kill_process("MetadataToStdoutProducer")

    def clean_node(self, node):
        node.account.ssh("rm -rf /mnt/producer.log")

    def try_parse_json(self, string):
        """Try to parse a string as json. Return None if not parseable."""
        try:
            record = json.loads(string)
            return record
        except ValueError:
            self.logger.debug("Could not parse as json: %s" % str(string))
            return None
