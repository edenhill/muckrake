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

from ducktape.tests.test import Test
from ducktape.tests.action import TestAction
from ducktape.tests.action_reporter import TextActionReporter
from ducktape.utils import wait
from muckrake.services.zookeeper import ZookeeperService
from muckrake.services.kafka import KafkaService
from muckrake.services.kafka_client import KafkaClientService, KafkaClient, KafkaProducer, KafkaSimpleConsumer

import concurrent.futures
import time
import inspect
import traceback
import uuid
import random
import os

from collections import defaultdict, OrderedDict

# Client implementations to test
CLIENT_IMPLEMENTATIONS=['sarama', 'kafka-node', 'kafka-python', 'pykafka']


class KafkaClientHandle(object):
    def __init__ (self, implname, logger):
        super(KafkaClientHandle, self).__init__()
        self.implname  = implname
        self.kc        = None  # KafkaClient
        self.p         = None  # KafkaProducer
        self.c         = None  # KafkaConsumer
        self.topic     = None
        self.logger    = logger

        
class KafkaClientBasicTest(Test):
    '''Generic client basic test
    '''
    def __init__ (self, test_context):
        super(KafkaClientBasicTest, self).__init__(test_context=test_context)
        self.test_context = test_context
        self.zk = ZookeeperService(test_context, 1)
        self.kafka = KafkaService(test_context, 1, self.zk)
        self.clservice = KafkaClientService(test_context, 1, self.kafka, self.zk)
        self.topic_id_next = 0
        self.clients = list()
        for implname in CLIENT_IMPLEMENTATIONS:
            ch = KafkaClientHandle(implname, self.logger)
            ch.kc = KafkaClient(implname, self.clservice)
            self.clients.append(ch)

    def mk_topic (self, name='cc_test', topic_cfg=None, unique=False, create=False):
        """ Constructs a topic name.
            unique := True: generate a unique topic name, False: topic may already exist
            create := True: create the topic before returning, False: dont create topic """
        if unique:
            suffix = '_%s' % str(uuid.uuid4())
        else:
            suffix = '_%d' % self.topic_id_next
            self.topic_id_next += 1

        tname = '%s%s' % (name, suffix)

        if create:
            if topic_cfg:
                tconf = topic_cfg
            else:
                tconf = {'topic': tname}

            self.kafka.create_topic(tconf)

        return tname

    def test(self):
        root = TestAction('client', desc='Basic client testing', logger=self.logger)

        # Container for client interop results
        # Schema:
        #     [ { 'action': TestAction,
        #         'res': [ { 'a': KafkaClientHandle, 'b': KafkaClientHandle, 'action': TestAction } ]
        #       } ]
        interop = list()

        # Start cluster
        self.zk.start()
        self.kafka.start()
        self.clservice.start()


        # Start client controller adapters
        self.logger.debug('Starting %d client controllers' % len(self.clients))
        for ch in self.clients:
            with TestAction('start', instance=ch.implname, desc='Start client controller', parent=root) as action:
                ch.kc.start()

        # For troubleshooting purposes output each controller's capabilities as a test action result
        caps_action = TestAction('capabilities', desc='Client capabilities', parent=root)
        for ch in self.clients:
            with TestAction('list', instance=ch.implname, parent=caps_action) as action:
                action.passed('%s' % ch.kc.cr.cc.caps)
        caps_action.done()

        # Verify that a version is reported
        version_action = TestAction('version', desc='Client implementation version', parent=root)
        for ch in self.clients:
            with TestAction('verify', instance=ch.implname, parent=version_action) as action:
                version = ch.kc.cr.cc.version
                if len(version) == 0 or version == 'FIXME':
                    action.failed('No/phony version reported ("%s")' % version)
                else:
                    action.passed('Version "%s" reported' % version)
        version_action.done()

        root_producer = TestAction('producer', desc='Basic producer testing', parent=root)

        for prodtype in ['sync','async']:
            with TestAction(prodtype, desc=prodtype + ' producer', parent=root_producer) as prodtype_action:
                producers = self.eligible_clients(prodtype + 'producer', self.clients)

                # Clients not explicitly supporting sync producers are okay, that is easy enough
                # to implement on top of an async producer, but clients not supporting
                # async producers are lacking in critical functionality and warrants a test failure.
                if prodtype == 'async':
                    not_async = [x for x in self.eligible_clients('syncproducer', self.clients) if x not in producers]
                    for ch in not_async:
                        action = TestAction('create', instance=ch.implname, parent=prodtype_action)
                        action.failed('Does not support ' + prodtype + ' producer')

                if len(producers) == 0:
                    prodtype_action.failed('No eligible %s producers' % prodtype)
                    continue

                ######################################################################
                # Produce messages to existing topics
                ######################################################################
                with TestAction('existingtopic',
                                desc='Produce to existing topic', parent=prodtype_action) as action:
                    self.do_test_producer_simple_with_verification(action, prodtype + 'producer',
                                                                   producers,
                                                                   unique_topics=True,
                                                                   create_topics=True,
                                                                   interop=interop)

                ######################################################################
                # Produce messages to non-existing topic with auto.topics.create=true
                # FIXME: verify auto.topics.create=true is actually set.
                ######################################################################
                with TestAction('nonexistingtopic',
                                desc='Produce to non-existing topic with automatic topic creation enabled',
                                parent=prodtype_action) as action:
                    self.do_test_producer_simple_with_verification(action, prodtype + 'producer',
                                                                   producers,
                                                                   unique_topics=True,
                                                                   create_topics=False,
                                                                   interop=interop)



        ######################################################################

        # Stop client controllers
        self.logger.debug('Stopping %d clients' % len(self.clients))
        for ch in self.clients:
            ch.kc.stop()

        root_producer.done()
        root.done()

        # FIXME: Create client interop too.
        #        This is a separate HTML file.
        self.interop_report(interop)

        return self.report()


    def report (self):
        results = TestAction.results()
        reporter = TextActionReporter()
        rep = reporter.report()
        self.logger.info('############################# Report ##############################')
        self.logger.info('\n%s' % rep)
        return {'actions': results}


    def interop_key (self, o):
        return o['a'].implname

    def interop_report (self, interop):
        # FIXME: proper this up.
        iop_file = os.path.join(self.test_context.session_context.results_dir, "interop.md")
        self.logger.info('THIS SHOULD BE WRITTEN TO %s' % iop_file)
        self.logger.info('############################### Interop report ########################')
        for o in interop:
            self.logger.info('Interop matrix for action: %s' % o['action'].fullname)

            # Iterate testing client (a) and verifying client (b), prepare them for
            # row and column positioning respectively.
            cols = dict()
            mtx = defaultdict(dict)
            for res in sorted(o['res'], key=self.interop_key):
                aname = res['a'].implname
                bname = res['b'].implname
                if bname not in cols:
                    cols[bname] = len(cols)
                mtx[aname][bname] = res['action'].status()

            # Column header
            s = ''
            for col in [''] + sorted(cols, key=cols.get):
                s += ' %22s |' % col
            self.logger.info(s)

            # Per row action status output
            for aname in mtx:
                s = ''
                # Per column output
                for b in [aname] + [mtx[aname][x] if x in mtx[aname] else '-' for x in sorted(cols, key=cols.get)]:
                    s += ' %22s |' % b
                self.logger.info(s)
            self.logger.info('---------------------------------------------------------------------')


    def eligible_clients (self, caps, clients):
        """ Returns a list of eligible clients matching all of the keywords """
        return [x for x in clients if x.kc.cr.cc != None and caps in x.kc.cr.cc.caps]

    def chlist_consumers_from_producers (self, action, chlist, avoid_same_impl=False,
                                         full_matrix=False):
        """ Returns a new chlist with corresponding consumers for each producer in 'chlist'.
            The consumer chlist will be set up to consume the same topic&partition the
            producer produced to, to verify the produced messages.
             avoid_same_impl := dont let producer and consumer share the same client implementation
             full_matrix     := let each producer be tested by each consumer.
        """
        ch_ret = list()

        # Create list of eligible consumers
        consumers = self.eligible_clients(caps='simpleconsumer', clients=self.clients)

        for o in chlist:
            ch = o['ch']

            if avoid_same_impl:
                elig = [x for x in consumers if x.implname != ch.implname]
            else:
                elig = consumers

            if not full_matrix:
                elig = random.choice(elig)

            if len(elig) == 0:
                action.warning('Failed to get a consumer for producer %s' % ch.implname)
                continue

            for consumer in elig:
                c_o = o.copy()
                c_o['ch'] = consumer
                c_o['producer_ch'] = ch
                self.logger.debug('Selected consumer %s for producer %s' % (consumer.implname, ch.implname))
                ch_ret.append(c_o)

        return ch_ret

    
    def consumer_start (self, ch, topic, partition, batch_id, offset, msg_cnt):
        """
        Start a consumer.
        Returns a (consumer, future, None) tuple or (None,None,errstr) on failure.
        Consumer is stopped with 'consumer_stop(c, f)'
        """
        c = KafkaSimpleConsumer(ch.kc)
        err = c.open()
        if err != None:
            c.close()
            return (None, None, 'Failed to create consumer: open failed: %s' % err)

        rep, f = c.consume(topic, partition, batch_id, offset, msg_cnt)
        if rep['err']:
            c.close()
            return (None, None, 'Failed to start consumer: %s' % rep['err'])

        return (c, f, None)

    def consumer_stop (self, c, f):
        """
        Stops a consumer 'c' with future 'f' previously started with consumer_start()
        """
        if f != None and not f.done():
            f.cancel()
        c.stop()



    def do_test_verifying_consumer (self, parent_action, chlist, interop=None):
        """ Runs a verifying consumer for each client in 'chlist' to verify
            that the corresponding producer actually produced the messages it said. """

        for o in chlist:
            ch = o['ch']
            self.logger.debug('For producer %s run verifying consumer %s on topic %s partition %d with batch_id %s' % \
                              (o['producer_ch'].implname, o['ch'].implname, o['topic'], o['partition'], o['batch_id']))
            o['c_action'] = TestAction('verifyingconsumer', instance=ch.implname, parent=o['p_action'])
            o['c'], o['f'], err = self.consumer_start(ch, o['topic'], o['partition'], o['batch_id'],
                                                      -2, o['count'])
            if err != None:
                o['c_action'].failed('%s' % err)
                del o['c_action']

        # Wait for completion
        while True:
            (o, res) = self.wait_futures([(x, x['f']) for x in chlist if x['f'] != None], timeout=10.0)
            if not o:
                # Timeout
                break

            o['f']   = None
            # Consumer and producer test actions
            # Not failing or passing these actions will indicate a DNF status in which case
            # the other (p in case of c, c in case of p) action will have errored out.
            c_action = o['c_action']
            p_action = o['p_action']

            # Verify results
            if res['err']:
                 c_action.failed('Consume failed: %s' % res['err'])
            elif 'errors' in res and res['errors']:
                c_action.failed('Consumer error(s): %s' % '\n'.join(res['errors']))
            elif 'count' in res and int(res['count']) > o['count']:
                c_action.failed('Consumed more messages (%d) than desired (%d): offset "newest" not working?' % (res['count'], o['count']))
            elif 'delivered' in res and int(res['delivered']) < o['count']:
                c_action.failed('Only %d/%d messages delivered' % (int(res['delivered']), o['count']))
            else:
                c_action.passed('Consumed %d/%d messages' % (int(res['count']), o['count']))
                p_action.passed('%d messages verified by %s consumer' % (o['count'], o['ch'].implname))

        # Fail any consumers that still havent finished.
        # Pass/Fail the producer action.
        for o in [x for x in chlist if x['f'] != None]:
            o['f'].cancel()
            o['f'] = None
            c_action = o['c_action']
            p_action = o['p_action']
            c_action.failed('Timed out waiting for consumer to finish')
            p_action.failed('Timed out waiting for verifying consumer %s to finish (action %s)' % \
                            (o['ch'].implname, c_action.fullname))

        if interop != None:
            iop = {'action': parent_action, 'res': list()}
            interop.append(iop)
        else:
            iop = None

        # Make sure results are symetrical.
        # I.e., consumer must not succeed if producer fail.
        # And generate interop results.
        for o in [x for x in chlist if 'c_action' in x]:
            c_action = o['c_action']
            p_action = o['p_action']
            if p_action.has_failed() and not c_action.has_failed():
                c_action.failed('Asymetrical test results: Consumer passed even though producer failed')
            if iop:
                iop['res'].append({'a': o['producer_ch'], 'b': o['ch'], 'action': c_action})


    def wait_futures (self, flist, timeout=10.0):
        """ Wait for any of the futures in 'flist' to finish.
            flist is a list of tuples: [(object, future), (object2, future2), ..]
            Call repeatedly until flist is empty.
            Returns a new tuple for a finished future (object, result), or (None,None) on timeout
        """
        timeout += time.time()
        while time.time() < timeout:
            for (o, f) in flist:
                try:
                    res = f.result(0.5 / len(flist))
                except concurrent.futures.TimeoutError:
                    continue
                except:
                    raise
                return (o,res)
        return (None,None)


    def do_test_producer_simple_with_verification (self, action, prodtype, producers, create_topics=True, unique_topics=False, interop=None):
        """ Simple producer tests with consumer verification """
        ############################################
        # Produce messages to existing topic.
        ############################################
        # Create a topic per client
        chlist = list()
        for ch in producers:
            chlist.append({'ch': ch, 'count': 10,
                           'topic': self.mk_topic(name='%s_%s' % (action.name, ch.implname), create=create_topics, unique=unique_topics)})

        self.do_test_producer_simple(action, prodtype, chlist)

        # Get list of consumer client handles for verifying the producers.
        chlist_cons = self.chlist_consumers_from_producers(action, chlist, full_matrix=True)
        if len(chlist_cons) < len(chlist):
            action.failed('Unable to acquire enough verifying consumers (producers: %s, consumers: %s)' % \
                          ([x['ch'].kc.name for x in chlist],
                           [x['ch'].kc.name for x in chlist_cons]))
        else:
            # Run verifying consumers
            self.do_test_verifying_consumer(action, chlist_cons, interop=interop)
 

    def do_test_producer_simple (self, parent_action, prodtype, chlist):
        """ Create producers, produce messages, wait for delivery, update chlist """

        # Create producer instances
        self.logger.debug('Creating %d %s producer instances' % (len(chlist), prodtype))
        for ch in [x['ch'] for x in chlist]:
            with TestAction('create', instance=ch.implname, parent=parent_action) as action:
                ch.p = KafkaProducer(ch.kc, prodtype)

                # Open Producer
                err = ch.p.open()
                if err != None:
                    ch.p.close()
                    action.failed('open failed: %s' % err)
                    continue

        wait_for = dict()
        # Produce messages
        self.logger.debug('Producing messages for %d producer instances' % len(chlist))

        for o in chlist:
            ch = o['ch']
            topic = o['topic']
            self.logger.debug('Producer %s producing to topic %s' % (ch.kc.name, topic))

            o['partition'] = 0
            o['action'] = TestAction('produce', instance=ch.implname, parent=parent_action,
                                     desc='Produce %d messages to single partition' % o['count'])
            o['batch_id'], o['f'] = ch.p.produce(topic=topic, partition=o['partition'], count=o['count'])
            wait_for[ch] = o

            # Create an action for the verifying consumer to use when verifying this producer
            o['p_action'] = TestAction('produce.verify', parent=o['action'], desc='Verifying messages delivered')

        # Wait for messages to be produced
        tmout = time.time() + 10
        while len(wait_for) > 0 and time.time() < tmout:
            for ch in wait_for.keys():
                o = wait_for[ch]
                try:
                    res = o['f'].result(0.5 / len(wait_for))
                except concurrent.futures.TimeoutError:
                    continue
                except:
                    raise

                del wait_for[ch]

                action = o['action']
                self.logger.debug('%s produce result: %s' % (ch.kc.name, res))

                if res['err']:
                    action.failed('Produce failed: %s' % res)
                    o['p_action'].failed('Produce failed, see %s' % action.fullname)
                    continue

                if res['errored'] > 0:
                    action.failed('Produce: %d/%d messages delivered, %d failed: %s' % \
                                  (res['delivered'], res['count'], res['errored'], '\n'.join(res['errors'])))
                    o['p_action'].failed('Produce failed, see %s' % action.fullname)
                    continue

                action.passed('Produce: %d/%d messages delivered' % \
                              (res['delivered'], res['count']))

        for ch in wait_for.keys():
            o = wait_for[ch]
            o['f'].cancel()
            o['action'].failed('Timed out waiting for produce results')

        # Close producer instances.
        self.logger.debug('Stopping %d producer instances' % len(self.clients))
        for ch in [x['ch'] for x in chlist]:
            ch.p.close()




