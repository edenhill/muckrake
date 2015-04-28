# Copyright 2014 Confluent Inc.
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

from .test import RestProxyTest
from muckrake.services.performance import ProducerPerformanceService, RestProducerPerformanceService,\
    ConsumerPerformanceService, RestConsumerPerformanceService


class NativeVsRestProducerPerformance(RestProxyTest):
    def __init__(self, test_context):
        super(NativeVsRestProducerPerformance, self).__init__(test_context, num_zk=1, num_brokers=1, num_rest=1, topics={
            'test-rep-one' : { 'partitions': 6, 'replication-factor': 1 },
        })

        if True:
            # Works on both aws and local
            msgs = 1000000
        else:
            # Can use locally on Vagrant VMs, but may use too much memory for aws
            msgs = 50000000

        msg_size = 100
        batch_size = 8196
        acks = 1

        self.services['producer_perf'] = ProducerPerformanceService(
            self.service_context(1), self.kafka,
            topic="test-rep-one", num_records=msgs, record_size=msg_size, throughput=-1,
            settings={'batch.size':batch_size, 'acks':acks}
        )
        self.producer_perf = self.services['producer_perf']

        self.services['rest_producer_perf'] = RestProducerPerformanceService(
            self.service_context(1), self.rest,
            topic="test-rep-one", num_records=msgs, record_size=msg_size, batch_size=batch_size, throughput=-1
        )
        self.rest_producer_perf = self.services['rest_producer_perf']

    def run(self):
        self.producer_perf.run()
        self.rest_producer_perf.run()

        self.logger.info("Producer performance: %f per sec, %f ms", self.producer_perf.results[0]['records_per_sec'], self.producer_perf.results[0]['latency_99th_ms'])
        self.logger.info("REST Producer performance: %f per sec, %f ms", self.rest_producer_perf.results[0]['records_per_sec'], self.rest_producer_perf.results[0]['latency_99th_ms'])


class NativeVsRestConsumerPerformance(RestProxyTest):
    def __init__(self, test_context):
        super(NativeVsRestConsumerPerformance, self).__init__(test_context, num_zk=1, num_brokers=1, num_rest=1, topics={
            'test-rep-one' : { 'partitions': 6, 'replication-factor': 1 }
        })

        if True:
            # Works on both aws and local
            msgs = 1000000
        else:
            # Can use locally on Vagrant VMs, but may use too much memory for aws
            msgs = 50000000

        msg_size = 100
        batch_size = 8196
        acks = 1 # default for REST proxy, which isn't yet configurable
        nthreads = 1 # not configurable for REST proxy

        self.services['producer'] = ProducerPerformanceService(
            self.service_context(1), self.kafka,
            topic="test", num_records=msgs+1000, record_size=msg_size, throughput=-1,
            settings={'batch.size':batch_size, 'acks': acks}
        )
        self.producer = self.services['producer']

        self.services['consumer_perf'] = ConsumerPerformanceService(
            self.service_context(1), self.kafka,
            topic="test", num_records=msgs, throughput=-1, threads=nthreads
        )
        self.consumer_perf = self.services['consumer_perf']

        self.services['rest_consumer_perf'] = RestConsumerPerformanceService(
            self.service_context(1), self.rest,
            topic="test", num_records=msgs, throughput=-1
        )
        self.rest_consumer_perf = self.services['rest_consumer_perf']

    def run(self):
        # Seed data. FIXME currently the REST consumer isn't properly finishing
        # unless we have some extra messages -- the last set isn't getting
        # properly returned for some reason.
        self.producer.run()

        self.consumer_perf.run()
        self.rest_consumer_perf.run()

        self.logger.info("Consumer performance: %f MB/s, %f msg/sec", self.consumer_perf.results[0]['mbps'], self.consumer_perf.results[0]['records_per_sec'])
        self.logger.info("REST Consumer performance: %f MB/s, %f msg/sec", self.rest_consumer_perf.results[0]['mbps'], self.rest_consumer_perf.results[0]['records_per_sec'])
