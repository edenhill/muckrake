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


class VerboseProducerService(BackgroundThreadService):
    def __init__(self, context, num_nodes, kafka, topic, num_records):
        super(VerboseProducerService, self).__init__(context, num_nodes)

        self.kafka = kafka
        self.args = {
            'topic': topic,
            'num_records': num_records
        }

        self.acked = []
        self.not_acked_data = []
        self.not_acked_values = []

    def _worker(self, idx, node):
        args = self.args.copy()
        args.update({'bootstrap_servers': self.kafka.bootstrap_servers()})
        cmd = "/opt/kafka/bin/kafka-run-class.sh kafka.tools.VerboseProducer --topic %(topic)s --broker-list %(bootstrap_servers)s" % args

        self.logger.debug("Verbose producer %d command: %s", idx, cmd)

        for line in node.account.ssh_capture(cmd):
            line = line.strip()

            data = self.try_parse_json(line)
            if data is not None:
                if hasattr(data, "exception"):
                    data["node"] = idx
                    self.not_acked.append(data)
                    self.not_acked_values.append(int(data["value"]))
                else:
                    self.acked.append(int(data["value"]))

            self.logger.debug("Verbose producer node %d: %s", idx, line.strip())
            print "Verbose producer node %d: %s" % (idx, line.strip())

        print "acked: ", self.acked
        # collect information on failures and successes

    def try_parse_json(self, str):

        try:
            record = json.loads(str)
            return record
        except ValueError:
            return None
