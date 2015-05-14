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


class RestProducerPerformanceService(PerformanceService):
    def __init__(self, context, num_nodes, rest, topic, num_records, record_size, batch_size, throughput, settings={}):
        super(RestProducerPerformanceService, self).__init__(context, num_nodes)
        self.rest = rest
        self.args = {
            'topic': topic,
            'num_records': num_records,
            'record_size': record_size,
            'batch_size': batch_size,
            # Because of the way this test tries to match the requested
            # throughput, we need to make sure any negative values are at least
            # batch_size
            'throughput': throughput if throughput > 0 else -batch_size
        }
        self.settings = settings

    def _worker(self, idx, node):
        args = self.args.copy()
        args.update({'rest_url': self.rest.url()})
        cmd = "/opt/kafka-rest/bin/kafka-rest-run-class io.confluent.kafkarest.tools.ProducerPerformance "\
              "'%(rest_url)s' %(topic)s %(num_records)d %(record_size)d %(batch_size)d %(throughput)d" % args
        for key,value in self.settings.items():
            cmd += " %s=%s" % (str(key), str(value))
        self.logger.debug("REST producer performance %d command: %s", idx, cmd)
        last = None
        for line in node.account.ssh_capture(cmd):
            self.logger.debug("REST producer performance %d: %s", idx, line.strip())
            last = line
        # Parse and save the last line's information
        parts = last.split(',')
        self.results[idx-1] = {
            'records': int(parts[0].split()[0]),
            'records_per_sec': float(parts[1].split()[0]),
            'mbps': float(parts[1].split('(')[1].split()[0]),
            'latency_avg_ms': float(parts[2].split()[0]),
            'latency_max_ms': float(parts[3].split()[0]),
            'latency_50th_ms': float(parts[4].split()[0]),
            'latency_95th_ms': float(parts[5].split()[0]),
            'latency_99th_ms': float(parts[6].split()[0]),
            'latency_999th_ms': float(parts[7].split()[0]),
        }


class RestConsumerPerformanceService(PerformanceService):
    def __init__(self, context, num_nodes, rest, topic, num_records, throughput, settings={}):
        super(RestConsumerPerformanceService, self).__init__(context, num_nodes)
        self.rest = rest
        self.args = {
            'topic': topic,
            'num_records': num_records,
            # See note in producer version. For consumer, must be as large as
            # the default # of messages returned per request, currently 100
            'throughput': throughput if throughput > 0 else -100
        }
        self.settings = settings

    def _worker(self, idx, node):
        args = self.args.copy()
        args.update({'rest_url': self.rest.url()})
        cmd = "/opt/kafka-rest/bin/kafka-rest-run-class io.confluent.kafkarest.tools.ConsumerPerformance "\
              "'%(rest_url)s' %(topic)s %(num_records)d %(throughput)d" % args
        for key, value in self.settings.items():
            cmd += " %s=%s" % (str(key), str(value))
        self.logger.debug("REST Consumer performance %d command: %s", idx, cmd)
        last = None
        for line in node.account.ssh_capture(cmd):
            self.logger.debug("REST Consumer performance %d: %s", idx, line.strip())
            last = line
        # Parse and save the last line's information
        self.results[idx-1] = parse_performance_output(last)