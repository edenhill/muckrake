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

import time


def make_message(i):
    """Create message given an index"""
    return str(i)


class ConsoleProducerService(BackgroundThreadService):

    def __init__(self, context, num_nodes, kafka, topic, message_func=make_message, max_time_seconds=900, max_messages=100000):
        super(ConsoleProducerService, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.args = {
            'topic': topic,
        }

        self.ready_to_finish = False
        self.message_maker = message_func
        self.max_time_seconds = max_time_seconds
        self.max_messages = max_messages

    def _worker(self, idx, node):

        args = self.args.copy()
        args.update({'broker_list': self.kafka.bootstrap_servers()})
        cmd = "/opt/kafka-0.8.1.1/bin/kafka-console-producer.sh "\
              "--topic %(topic)s --broker-list %(broker_list)s" % args

        self.logger.debug("starting console producer with command: %s" %cmd)
        stop_time = time.time() + self.max_time_seconds
        i = 0
        proc = node.account.ssh_in(cmd)
        while time.time() < stop_time and i < self.max_messages and not self.ready_to_finish:
            msg = self.message_maker(i)
            proc.stdin.write(str(msg) + "\n")
            i += 1
        proc.communicate()

    def start_node(self, node):
        self.ready_to_finish = False
        super(ConsoleProducerService, self).start_node(node)

    def stop_node(self, node):
        self.ready_to_finish = True
        node.account.kill_process("java")

"""
0.8.1.1 console producer options

--batch-size
--broker-list
--key-serializer -- default kafka.serializer.StringEncoder
--line-reader default: kafka. producer.ConsoleProducer$LineMessageReader
--message-send-max-retries -- default 3
--property key=value
--queue-enqueuetimeout-ms
--queue-size
--request-required-acks -- default 0
--request-timeout-ms -- default 1500
--retry-backoff-ms -- default 100
--socket-buffer-size-- default: 102400
--sync
--timeout default: 1000
--topic REQUIRED
--value-serializer default: kafka.serializer.StringEncoder


--compress true/false
"""

"""
0.8.2.1 options

--batch-size
--broker-list -- REQUIRED
--key-serializer -- default: kafka. serializer.DefaultEncoder
--line-reader -- default: kafka. tools.ConsoleProducer$LineMessageReader
--message-send-max-retries
--property key=value
--queue-enqueuetimeout-ms
--queue-size
--request-required-acks
--request-timeout-ms
--retry-backoff-ms
--socket-buffer-size
--sync
--timeout
--topic -- REQUIRED
--value-serializer


--compression-codec -- default: 'gzip'
--max-memory-bytes
--max-partition-memory-bytes
--metadata-expiry-ms
--metadata-fetch-timeout-ms
--new-producer      ******
"""



