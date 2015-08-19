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


class TestActionsTest (Test):
    ''' Tests the ducktape TestAction class '''

    def __init__ (self, test_context):
        super(TestActionsTest, self).__init__(test_context)



    def test(self):
        root = TestAction('testaction', desc='Basic TestAction testing', logger=self.logger)

        with TestAction('subtest1', desc='In a with-clause', parent=root) as action:
            action.passed('All good')

        with TestAction('subtest2', desc='In a with-clause', parent=root) as action:
            with TestAction('willfail', desc='This one will fail', parent=action) as action2:
                for i in range(1,10):
                    action2.failed('This is failure #%d' % i)

            with TestAction('willfail.too', desc='This one will fail too, through exceptions', parent=action) as action3:
                i = 100 / 0

            with TestAction('implicit.success', desc='This one will succeed implicitly', parent=action) as action4:
                pass


        # Return action results
        return {'actions': TestAction.results()}
