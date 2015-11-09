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

#
# Kafka Client testing
#

from ducktape.services.service import Service
import kafka_client_ctrl
from urlparse import urlparse
from collections import defaultdict
import re


class KafkaClientService(Service):
    """ Kafka KafkaClientService provides a service for provisioning and controlling
        multiple Kafka clients using the client controller infrastructure in kafka_client_ctrl """

    # The log file list is updated as new client controllers are added.
    logs = { }
    
    def __init__(self, context, num_nodes, kafka, zk):
        """ context := TestContext
            num_nodes := nodes required
            kafka := KafkaService
            zk := ZookeeperService
        """
        super(KafkaClientService, self).__init__(context, num_nodes)
        self.zk = zk
        self.kafka = kafka
        self.clients = list()
        self.port_next = 7700
        self.node_usage = defaultdict(list)

        
    def add_log (self, lname, path, collect_default=True):
        """ Add client log file to path of collectables """
        self.logs[lname] = {'path': path, 'collect_default': collect_default}

    def start (self):
        self.logger.debug('KafkaClientService start')
        super(KafkaClientService, self).start()

    def allocate_node (self, client):
        """ Allocates (possibly shared) node. """
        # Use one node for all clients in this service, for now. 
        node = self.nodes[0]
        self.node_usage[node].append(client)
        self.logger.info('%s: Allocated node %s' % (client.name, node.account))
        return node

    def allocate_port (self, node):
        """ Allocate free port on node """
        # FIXME: per-node allocations
        port = self.port_next
        self.port_next += 1
        self.logger.info('Allocated port %d on %s' % (port, node.account))
        return port

    def start_node (self, node):
        self.logger.debug('KafkaClientService start_node %s' % str(node.account))
        pass

    def stop_node (self, node):
        self.logger.debug('KafkaClientService stop_node %s' % str(node.account))

        # Stop all clients for this node
        for client in self.node_usage[node]:
            client.stop()
        if len(self.node_usage[node]) > 0:
            self.logger.error('The following clients failed to stop on node %s: %s' % \
                              (node.account, ', '.join([str(x) for x in self.node_usage[node]])))

    def clean_node (self, node):
        """ Clean up after clients on node """
        node.account.ssh("pkill -f '.*clientctrl_.*'", allow_fail=True)
        node.account.ssh("rm -f /tmp/clientctrl_*.pid /mnt/clientctrl_*.log")

    def add_client (self, client):
        """ Add client """
        self.clients.append(client)

    def del_client (self, client):
        """ Delete client, client should've been stopped first. """
        for node in self.node_usage:
            if client in self.node_usage[node]:
                self.node_usage[node].remove(client)
        if client in self.clients:
            self.clients.remove(client)





class KafkaClient (object):
    """ A single Kafka client instance provisioned with kafka_client_ctrl """


    # Global list of clients
    clients = list()

    @staticmethod
    def get (name_match=None):
        """ Retrieve list of client's matching regexp 'name_match' (or all if None) """
        if not name_match:
            patt = r'.*'
        else:
            patt = name_match
        return [x for x in KafkaClient.clients if re.match(patt, x.name)]

    def __init__ (self, name, clservice):
        """ Creates a new Kafka client
               name      := client type name, e.g. 'sarama'
               clservice := a KafkaClientService instance
        """
        super(KafkaClient, self).__init__()
        self.implname = 'clientctrl_%s' % name
        self.name = '%s_%d' % (self.implname, len(self.clients)+1)
        self.clservice = clservice
        self.clservice.add_client(self)
        self.node = None
        self.port = -1
        self.url = None
        self.cc = None
        self.logger = clservice.logger
        self.clients.append(self)
        # Designate log file
        self.logfile = "/mnt/%s.log" % self.name
        self.clservice.add_log(self.name, self.logfile, collect_default=True)


    def __repr__ (self):
        return '<%s(name=%s, url=%s)>' % (self.__class__, self.name, self.url)

    def alive (self):
        return self.node is not None

    def delete (self):
        """ Remove from KafkaClienTService """
        self.clservice.del_client(self)


    def setup (self, node):
        """ Set up / prepare client implementation on node
            This should be close to a no-op if all prereqs are already setup, but
            we leave that logic to the setup.sh script to figure out. """
        self.logger.debug('Setting up %s instance on %s' % (self.name, node.account))
        node.account.ssh('/opt/%s/setup.sh' % (self.implname))
        self.node = node
        
    def start (self):
        """ Allocate a node and start the Kafka client """
        node = self.clservice.allocate_node(self)
        if not node:
            raise Exception('No nodes available for client %s' % self.name)

        self.url = 'ws://%s:%d/ws' % (node.account.hostname, self.clservice.allocate_port(node))
        self.port = int(urlparse(self.url).port)

        self.logger.info('Start client %s on URL %s' % (self.name, self.url))
        
        # Starts a client control adapter on the specified node and
        # creates a ClientCtrl instance for it.
        
        # Make sure implementation is set up correctly on node.
        self.setup(node)

        # Start instance
        self.logger.debug('Starting %s (%s) instance on %s' % (self.name, self.url, self.node.account))
        self.node.account.ssh('/opt/%s/start.sh %d %s > /tmp/clientctrl_%s.pid' % (self.implname, self.port, self.logfile, self.name))

        # Open client ctrl connection
        self.cc = kafka_client_ctrl.KafkaClientCtrl(self)
        if not self.cc.open():
            raise Exception('Failed to start client ctrl %s' % self.name)

        self.logger.debug('Adapter %s (%s) up and running' % (self.cc.name, self.cc.caps))


    def stop (self):
        """ Stops a client control adapter on the node it is running, also
            deconstructs the corresponding ClientCtrl instance. """
        if self.node:
            self.cc.close()
            self.cc = None
            self.node.account.ssh('kill `cat /tmp/clientctrl_%s.pid`' % self.name, allow_fail=True)
            self.node = None
        self.delete()


class KafkaProducer (kafka_client_ctrl.KafkaClientCtrlProducer):
    """ Kafka Producer instance """
    def __init__  (self, kc, prodtype):
        """ Producer instance for KafkaClient 'kc' """
        super(KafkaProducer, self).__init__(kc.cc, prodtype)
        self.kc = kc

    def open (self, config={}):
        """ Open/Create instance on adapter """
        conf = config.copy()
        conf['config'] = { 'metadata.broker.list': self.kc.clservice.kafka.bootstrap_servers(),
                           'zookeeper.connect': self.kc.clservice.zk.connect_setting()}
        super(KafkaProducer, self).open(conf)


class KafkaSimpleConsumer (kafka_client_ctrl.KafkaClientCtrlSimpleConsumer):
    def __init__  (self, kc):
        """ Consumer instance for KafkaClient 'kc' """
        super(KafkaSimpleConsumer, self).__init__(kc.cc)
        self.kc = kc

    def open (self, config={}):
        """ Open/Create instance on adapter """
        conf = config.copy()
        conf['config'] = { 'metadata.broker.list': self.kc.clservice.kafka.bootstrap_servers(),
                           'zookeeper.connect': self.kc.clservice.zk.connect_setting()}
        super(KafkaSimpleConsumer, self).open(conf)




