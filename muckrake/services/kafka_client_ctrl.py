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
# Kafka Client controller implementation
#

import websocket
import json
from concurrent import futures
import threading
import time
import sys
import Queue
import re
import uuid
import traceback
import logging
import random


class KafkaClientCtrlCmd (object):
    """ KafkaClientCtrl: Command request """
    def __init__ (self, msgtype, data, f, fn):
        super(KafkaClientCtrlCmd, self).__init__()
        self.msgtype = msgtype
        self.data = data
        self.f = f # future
        self.fn = fn  # handler

class KafkaClientCtrlComms (object):
    """ KafkaClientCtrl: communication base class """
    def __init__(self):
        pass

class KafkaClientCtrlWebsocket (KafkaClientCtrlComms):
    """ KafkaClientCtrl: websocket communication class """
    def __init__(self, cc, url, handle_push=None):
        """ cc := KafkaClientCtrl
            url := adapter URL (ws://host:port/ws)
            handle_push := callback for push notifications
        """
        super(KafkaClientCtrlWebsocket, self).__init__()
        self.cc = cc
        self.id_next = 1
        self.wait_reply = dict()
        self.handle_push = handle_push
        self.q = Queue.Queue()
        self.q_run = threading.Event()
        self.q_run.set()
        self.ws_run = threading.Event()
        self.ws_run.set()
        self.ws_opened = threading.Event()
        self.ws_opened.clear()
        self.ws = None

        # ws_started is used to signal back to this thread that main thread has started
        self.ws_started = threading.Event()
        
        # Create websocket app main thread: used for connections, ws events and received messages
        self.ws_worker = threading.Thread(name=str(self.__class__),
                                          target=self._ws_main,
                                          args={url})
        self.ws_worker.start()
        # Wait for start indication
        if not self.ws_started.wait(20.0):
            raise Exception("websocket thread did not start")

        # Create websocket queue/send thread
        self.q_worker = threading.Thread(name=str(self.__class__) + '_q',
                                          target=self._q_main,
                                          args={})
        self.q_worker.start()

        
    def close (self):
        """ Close WebSocket controller """
        # Tell threads to stop
        self.ws_run.clear()
        self.q_run.clear()
        # Close ws socket
        self.ws.close()
        # Wait for threads to exit
        self.ws_worker.join()
        self.q_worker.join()


    def _q_main (self, *args):
        """ Worker queue main thread loop """
        while self.q_run.is_set():
            try:
                if not self.ws_opened.wait(0.5):
                    continue

                cmd = self.q.get(True, 0.5)
            except Queue.Empty:
                continue
            # Call command function
            cmd.fn(cmd)

    def _ws_on_open (self, *args):
        """ Called on websocket connection open """
        self.cc.logger.info("connected")
        self.ws_opened.set()

    def _ws_on_close (self, *args):
        """ Called on websocket connection close """
        if not self.ws_opened.is_set():
            return
        self.cc.logger.warning("connection closed");
        self.ws_opened.clear()

    def _ws_on_message (self, ws, msg):
        """ Called on received websocket message """
        self.parse(msg)

    def _ws_main (self, url):
        """ WebSocket main thread: runs the websocket app dispatcher """
        websocket.enableTrace(False)
        self.cc.logger.info("%s: Connect websocket to %s" % (self.cc.kc.name, url))
        self.ws = websocket.WebSocketApp(url,
                                         on_message=self._ws_on_message,
                                         on_close=self._ws_on_close)
        self.ws.KafkaClientCtrl = self
        self.ws.on_open = self._ws_on_open
        self.cc.logger.debug("websocket wait open")
        self.ws_started.set()
        while self.ws_run.is_set():
            # ping_timeout makes sure the forever loop is interrupted by close()
            self.ws.run_forever(ping_timeout=0.5)
            # Avoid busy loop on websocket failure
            time.sleep(0.1)
        self.ws.close()


    def send0 (self, cmd, is_req=False):
        """ Low level web socket send: puts 'cmd' object in request envelope
            and transmits to remote adapter.
            is_req := Is request """
        envelop = {'msgtype': cmd.msgtype, cmd.msgtype: cmd.data, 'id': self.id_next }
        if is_req:
            reqid = self.id_next
            self.id_next += 1
            self.wait_reply[reqid] = cmd
        try:
            self.cc.logger.debug('%s: Sending %s' % (self.cc.name, envelop))
            self.ws.send(json.dumps(envelop))
        except Exception as e:
            cmd = self.wait_reply[reqid]
            del self.wait_reply[reqid]
            self.handle_reply(cmd, {}, 'Send failed: %s' % e)

    def req (self, cmd):
        """ Helper for sending request """
        return self.send0(cmd, is_req=True)

    def send (self, cmd):
        """ Helper for sending non-request """
        self.send0(cmd, is_req=False)

    def parse (self, msg):
        """ Parse received message and pass on to handler """
        try:
            m = json.loads(msg)
            if 'error' in m:
                err = m['error']
            else:
                err = None
            msgtype = m['msgtype']
            if msgtype in m:
                data = m[msgtype]
            else:
                data = None

            if 'repid' in m and m['repid'] != 0:
                # Is a reply, try map to request
                repid = m['repid']
                if repid in self.wait_reply:
                    cmd = self.wait_reply[repid]
                    del self.wait_reply[repid]
                    self.handle_reply(cmd, data, err)
                else:
                    self.cc.logger.warning('reply to unknown request: %s' % m)
            else:
                # Push/Notification: pass to handler
                self._handle_push(msgtype, data, err)

        except ValueError, e:
            self.cc.logger.error('Failed to parse received message: %s: %s' % (e, msg))
            return

        except:
            print traceback.format_exc()
            raise

    def _handle_push (self, msgtype, data=None, err=None):
        """ Handle a pushed message (unsolicited) from the client """
        if not data:
            data = dict()
        data['err'] = err

        if self.handle_push:
            self.handle_push(msgtype, data)

    def handle_reply (self, cmd, reply=None, err=None):
        """ Handle a request's reply """
        if not reply:
            reply = dict()
        reply['err'] = err
        cmd.f.set_result(reply)

    def enq (self, cmd):
        """ Enqueue command with cmd.fn callback for execution on queue thread.
            This is used to enqueue commands from non-websocket threads 
            for transmission on websocket thread. """
        self.q.put(cmd)


class GenericExecutor (futures.Executor):
    """ Generic concurrent.futures executor without an actual execution engine, that is to be
        handled by the application through other means (such as a Websocket thread).
        This is for providing a simple generic future primitive. """

    def __init__ (self):
        super(GenericExecutor, self).__init__()

    def submit (self, fn=None, *args, **kwargs):
        f = futures._base.Future()
        return f

    def shutdown (self, wait=True):
        return

class KafkaClientCtrl(object):
    """ Kafka Client controller """
    def __init__(self, kc):
        """ 'kc' is a KafkaClient instance """
        super(KafkaClientCtrl, self).__init__()
        self.logger = kc.logger
        self.kc = kc
        self.executor = GenericExecutor()
        self.comms = KafkaClientCtrlWebsocket(self, kc.url, handle_push=self.handle_push)
        self.test_id = str(uuid.uuid1())[0:7] # FIXME: dont trunc
        self.batch_id_next = random.randint(1, 1000000) * 1000
        self.batches = dict()
        self.notify_id_next = self.batch_id_next
        self.notifications = dict()
        self.topic_id_next = 1
        self.topics = list()
        self.caps = list()
        self.name = 'UNKNOWN'
        self.logger.debug('created')

    def __repr__ (self):
        return '<%s(url=%s, name=%s)>' % (self.__class__, self.kc.url, self.name)

    
    def handle_push (self, msgtype, data):
        """ Handle push/notification from adapter """
        if 'nid' not in data:
            self.logger.error('No notification id (nid) in %s push: %s' % (msgtype, data))
            return
        notify_id = data['nid']
        if notify_id not in self.notifications:
            self.logger.error('Received unknown notification id %d in %s push: %s' % (notify_id, msgtype, data))
            return
        f = self.notifications[notify_id]
        f.set_result(data)
        del self.notifications[notify_id]


    def req (self, msgtype, data):
        """ Send request to adapter """
        f = self.executor.submit()
        self.comms.enq(KafkaClientCtrlCmd(msgtype, data, f, self.comms.req))
        return f

    def req_listener (self, msgtype, data, timeout=10.0):
        """ Send a request and set up a future to listen for notifications.
            The initial request will be blocking.
            Returns (reply, future) """
        notify_id = self.notify_id_next
        self.notify_id_next += 1
        data['nid'] = notify_id

        # Create new future for collecting notifications.
        f = self.executor.submit()
        self.notifications[notify_id] = f

        try:
            # Send request, wait for response.
            rep = self.req(msgtype, data).result(timeout)
        except futures.TimeoutError:
            rep = {'err': 'Request timed out'}
        if rep['err']:
            self.logger.warning('%s failed immediately: %s' % (msgtype, rep['err']))
            try:
                del self.notifications[notify_id]
            except:
                pass
            f.set_result(rep)
        return (rep, f)

    
    def open (self):
        """ Open client controller communication.
            This is a blocking operation and returns when the connection has been facilitated
            and the initial handshake has been performed.
            Returns True on success or False on error
        """
        self.logger.debug('connecting')
        try:
            rep = self.req('hello', {'progname':sys.argv[0], 'clientname':'controller', 'version': 'FIXME', 'capabilities': 'controller'}).result(2.0)
        except futures.TimeoutError:
            rep = {'err': 'Request timed out'}
        if rep['err']:
            self.logger.error('%s: Open error: %s' % (self.kc.name, rep['err']))
            return False
        if 'clientname' not in rep or 'capabilities' not in rep:
            self.logger.error('Client responded with incompatible hello reply: %s' % rep)
            return False

        self.name = rep['clientname']
        # Derive a safe name for topic creation, etc.
        self.safename = re.sub(r'[^a-zA-Z0-9_-]+', '_', self.name)
        self.caps = rep['capabilities'].split(',')
        self.version = str(rep['version'])
        self.logger.info('Connected to client %s (%s) with capabilities %s' % (self.name, self.version, ','.join(self.caps)))
        return True

    def close (self):
        """ Clise client controller communication """
        self.logger.debug('Closing')
        self.comms.close()
        self.logger.info('Disconnected from client')

        
    def new_topic (self, topic_suffix='test'):
        """ Generate a new unique topic name for this client controller """
        tname = 'cc_%s_%i_%s' % (self.safename, self.topic_id_next, topic_suffix)
        self.topic_id_next += 1
        self.topics.append(tname)
        return tname

    def new_batch (self):
        """ Returns a new batch_id """
        bid = self.batch_id_next
        self.batch_id_next += 1
        return bid


class KafkaClientCtrlInstance (object):
    """ Client controller instance: syncproducer, asyncproducer, simpleconsumer, etc.. """
    def __init__ (self, cc, ctype):
        """ cc := KafkaClientCtrl
            ctype := client instance type, e.g., "syncproducer"
        """
        self.cc = cc
        self.ctype = ctype
        self.iid = -1

    def __repr__ (self):
        return '<%s(url=%s, ctype=%s, iid=%d)>' % (self.__class__, self.cc.kc.url, self.ctype, self.iid)

    def open (self, config={}):
        """ Create/open new instance with provided (optional) config.
            This is a blocking operation and returns an error string on failure or None on success
        """
        try:
            rep = self.cc.req('create', {'type': self.ctype, 'test_id': self.cc.test_id, 'config': config}).result(2)
        except futures.TimeoutError:
            rep = {'err': 'Request timed out'}
        if rep['err']:
            return 'Failed to create %s: Remote error: %s' % (self.ctype, rep['err'])
        if 'iid' not in rep:
            return 'Failed to create %s: missing iid' % self.ctype
        self.iid = rep['iid']
        self.cc.logger.info('Created %s with instance-id %s' % (self.ctype, self.iid))
        return None

    def close (self):
        """ Close/delete a client instance.
            This is a blocking operation """
        self.cc.logger.info('%s: Closing %s with instance-id %s' % (self.cc.kc.name, self.ctype, self.iid))
        try:
            rep = self.cc.req('delete', {'iid': self.iid}).result(5)
        except futures.TimeoutError:
            rep = {'err': 'Request timed out'}
        if rep['err']:
            self.cc.logger.warning('%s: Failed to close %s instance %s: %s' % (self.cc.kc.name, self.ctype, self.iid, rep['err']))
        self.iid = None


class KafkaClientCtrlProducer (KafkaClientCtrlInstance):
    """ Client control: producer instance """
    def __init__ (self, cc, prodtype='asyncproducer'):
        """ Create new producer instance on controlled client
            cc := KafkaClientCtrl
            prodtype := 'syncproducer' or 'asyncproducer'
        """
        super(KafkaClientCtrlProducer, self).__init__(cc, prodtype)

    def produce (self, topic, partition=-1, data=None, count=1, batch_id=-1):
        """ Produce 'count' messages to 'topic' & 'partition' on batch 'batch_id' with
            data pattern 'data'. If no batch_id is specified a new one will be generated.
            The actual produce delivery results are eventually returned by the future.
            Returns (batch_id, future_for_completion) """
        if batch_id == -1:
            _batch_id = self.cc.new_batch()
        else:
            _batch_id = batch_id
        (rep, f) = self.cc.req_listener('produce',
                                        {'iid': self.iid, 'bid': _batch_id,
                                         'topic': topic, 'partition': partition,
                                         'data': data, 'count': count})
        return (_batch_id, f)


class KafkaClientCtrlConsumer (KafkaClientCtrlInstance):
    """ Client control: consumer instance """
    def __init__(self, cc, ctype):
        """ Create new consumer instance on controlled client
            cc := KafkaClientCtrl
            ctype := 'simpleconsumer', ..
        """
        super(KafkaClientCtrlConsumer, self).__init__(cc, ctype)



class KafkaClientCtrlSimpleConsumer (KafkaClientCtrlConsumer):
    """ Client control: simpleconsumer instance """
    def __init__(self, cc):
        """ Create new simpleconsumer instance
            cc := KafkaClientCtrl
        """
        super(KafkaClientCtrlSimpleConsumer, self).__init__(cc, 'simpleconsumer')

    def consume (self, topic, partition, batch_id, offset, msg_cnt):
        """ Consume 'msg_cnt' messages from 'topic' & 'partition' at offset 'offset'
            (logical offsets -1 and -2 are supported).
            Match received messages with 'batch_id'.
        The actual results are eventually returned by the future.
        Returns (reply, future_for_completion)
        """
        (rep, f) = self.cc.req_listener('consume',
                                        {'iid': self.iid, 'bid': batch_id,
                                         'topic': topic, 'partition': partition,
                                         'expect_cnt': msg_cnt, 'timeout': 10,
                                         'offset': offset})
        return (rep, f)



