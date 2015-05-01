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

import threading


class BackgroundThreadService(Service):
    def __init__(self, context, num_nodes):
        super(BackgroundThreadService, self).__init__(context, num_nodes)
        self.worker_threads = []

    def start_node(self, node):
        idx = self.idx(node)

        self.logger.info("Running %s node %d on %s", self.__class__.__name__, idx, node.account.hostname)
        worker = threading.Thread(
            name=self.__class__.__name__ + "-worker-" + str(idx),
            target=self._worker,
            args=(idx, node)
        )
        worker.daemon = True
        worker.start()
        self.worker_threads.append(worker)

    def wait(self):
        super(BackgroundThreadService, self).wait()
        for idx, worker in enumerate(self.worker_threads, 1):
            self.logger.debug("Waiting for worker thread %s finish", worker.name)
            worker.join()
        self.worker_threads = None

    def stop_node(self, node):
        # do nothing
        pass

    def clean_node(self, node):
        # do nothing
        pass
