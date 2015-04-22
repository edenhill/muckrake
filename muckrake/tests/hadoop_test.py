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
from .test import HadoopTest
from muckrake.services.performance import HadoopPerformanceService


class HadoopV1SetupTest(HadoopTest):

    def __init__(self, test_context):
        super(HadoopV1SetupTest, self).__init__(test_context, 2, hadoop_distro='cdh', hadoop_version=1)
        self.services['hadoop_perf'] = HadoopPerformanceService(self.service_context(1), self.hadoop)
        self.hadoop_perf = self.services['hadoop_perf']

    def run(self):
        self.hadoop_perf.run()


class HadoopV2SetupTest(HadoopTest):

    def __init__(self, test_context):
        super(HadoopV2SetupTest, self).__init__(test_context, 2, hadoop_distro='cdh', hadoop_version=2)
        self.services['hadoop_perf'] = HadoopPerformanceService(self.service_context(1), self.hadoop)
        self.hadoop_perf = self.services['hadoop_perf']

    def run(self):
        self.hadoop_perf.run()


class HDPSetupTest(HadoopTest):

    def __init__(self, test_context):
        super(HDPSetupTest, self).__init__(test_context, 2, hadoop_distro='hdp', hadoop_version=2)
        self.services['hadoop_perf'] = HadoopPerformanceService(self.service_context(1), self.hadoop)
        self.hadoop_perf = self.services['hadoop_perf']

    def run(self):
        self.hadoop_perf.run()
