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

import re


def is_message(msg):
    """Default method used to check whether text pulled from console consumer is a message.

    Some of the text pulled from stdout of console consumer process is log output etc, so this provides a
    default way to validate that a line pulled from stdout is a message.

    It simply checks that the string is a sequence of digits.
    """
    return re.match('^\d+$', msg) is not None


class ConsoleConsumerService(BackgroundThreadService):

    logs = {
        "consumer_log": {
            "path": "/mnt/consumer.log",
            "collect_default": True}
        }

    def __init__(self, context, num_nodes, kafka, topic, message_validator=is_message, from_beginning=True, consumer_timeout_ms=None):
        super(ConsoleConsumerService, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.args = {
            'topic': topic,
        }

        self.consumer_timeout_ms = consumer_timeout_ms

        self.ready_to_finish = False
        self.from_beginning = from_beginning
        self.message_validator = message_validator
        self.messages_consumed = {idx: [] for idx in range(1, num_nodes + 1)}


    @property
    def start_cmd(self):
        args = self.args.copy()
        args.update({'zk_connect': self.kafka.zk.connect_setting()})
        cmd = "/opt/kafka/bin/kafka-console-consumer.sh "\
              "--topic %(topic)s --zookeeper %(zk_connect)s --consumer.config /mnt/console_consumer.properties" % args

        if self.from_beginning:
            cmd += " --from-beginning"

        cmd += " 2>> /mnt/consumer.log | tee -a /mnt/consumer.log &"

        return cmd

    def _worker(self, idx, node):
        # form config file
        if self.consumer_timeout_ms is not None:
            prop_file = self.render('console_consumer.properties', consumer_timeout_ms=self.consumer_timeout_ms)
        else:
            prop_file = self.render('console_consumer.properties')
        node.account.create_file("/mnt/console_consumer.properties", prop_file)

        # Run and capture output
        cmd = self.start_cmd
        self.logger.debug("Console consumer %d command: %s", idx, cmd)
        for line in node.account.ssh_capture(cmd, allow_fail=False, timeout=10):
            msg = line.strip()
            if self.message_validator(msg):
                self.logger.debug("consumed a message: " + msg)
                self.messages_consumed[idx].append(msg)
            if self.ready_to_finish:
                break

    def start_node(self, node):
        self.ready_to_finish = False
        super(ConsoleConsumerService, self).start_node(node)

    def stop_node(self, node):
        self.ready_to_finish = True
        node.account.kill_process("java", allow_fail=True)

    def clean_node(self, node):
        node.account.ssh("rm -rf /mnt/console_consumer.properties /mnt/consumer.log", allow_fail=True)


"""
0.8.1.1 ConsoleConsumer options


Option                                  Description
------                                  -----------
--autocommit.interval.ms <Integer: ms>  The time interval at which to save the
                                          current offset in ms (default: 60000)
--blacklist <blacklist>                 Blacklist of topics to exclude from
                                          consumption.
--consumer-timeout-ms <Integer: prop>   consumer throws timeout exception
                                          after waiting this much of time
                                          without incoming messages (default:
                                          -1)
--csv-reporter-enabled                  If set, the CSV metrics reporter will
                                          be enabled
--fetch-size <Integer: size>            The amount of data to fetch in a
                                          single request. (default: 1048576)
--formatter <class>                     The name of a class to use for
                                          formatting kafka messages for
                                          display. (default: kafka.consumer.
                                          DefaultMessageFormatter)
--from-beginning                        If the consumer does not already have
                                          an established offset to consume
                                          from, start with the earliest
                                          message present in the log rather
                                          than the latest message.
--group <gid>                           The group id to consume on. (default:
                                          console-consumer-96968)
--max-messages <Integer: num_messages>  The maximum number of messages to
                                          consume before exiting. If not set,
                                          consumption is continual.
--max-wait-ms <Integer: ms>             The max amount of time each fetch
                                          request waits. (default: 100)
--metrics-dir <metrics dictory>         If csv-reporter-enable is set, and
                                          this parameter isset, the csv
                                          metrics will be outputed here
--min-fetch-bytes <Integer: bytes>      The min number of bytes each fetch
                                          request waits for. (default: 1)
--property <prop>
--refresh-leader-backoff-ms <Integer:   Backoff time before refreshing
  ms>                                     metadata (default: 200)
--skip-message-on-error                 If there is an error when processing a
                                          message, skip it instead of halt.
--socket-buffer-size <Integer: size>    The size of the tcp RECV size.
                                          (default: 2097152)
--socket-timeout-ms <Integer: ms>       The socket timeout used for the
                                          connection to the broker (default:
                                          30000)
--topic <topic>                         The topic id to consume on.
--whitelist <whitelist>                 Whitelist of topics to include for
                                          consumption.
--zookeeper <urls>                      REQUIRED: The connection string for
                                          the zookeeper connection in the form
                                          host:port. Multiple URLS can be
                                          given to allow fail-over.

"""




"""
0.8.2.1 ConsoleConsumer options

The console consumer is a tool that reads data from Kafka and outputs it to standard output.
Option                                  Description
------                                  -----------
--blacklist <blacklist>                 Blacklist of topics to exclude from
                                          consumption.
--consumer.config <config file>         Consumer config properties file.
--csv-reporter-enabled                  If set, the CSV metrics reporter will
                                          be enabled
--delete-consumer-offsets               If specified, the consumer path in
                                          zookeeper is deleted when starting up
--formatter <class>                     The name of a class to use for
                                          formatting kafka messages for
                                          display. (default: kafka.tools.
                                          DefaultMessageFormatter)
--from-beginning                        If the consumer does not already have
                                          an established offset to consume
                                          from, start with the earliest
                                          message present in the log rather
                                          than the latest message.
--max-messages <Integer: num_messages>  The maximum number of messages to
                                          consume before exiting. If not set,
                                          consumption is continual.
--metrics-dir <metrics dictory>         If csv-reporter-enable is set, and
                                          this parameter isset, the csv
                                          metrics will be outputed here
--property <prop>
--skip-message-on-error                 If there is an error when processing a
                                          message, skip it instead of halt.
--topic <topic>                         The topic id to consume on.
--whitelist <whitelist>                 Whitelist of topics to include for
                                          consumption.
--zookeeper <urls>                      REQUIRED: The connection string for
                                          the zookeeper connection in the form
                                          host:port. Multiple URLS can be
                                          given to allow fail-over.
"""





