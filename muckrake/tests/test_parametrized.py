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
from ducktape.decorate import parametrize
from ducktape.decorate import matrix


def stringify_kwargs(*args, **kwargs):
    return ", ".join(["%s=%s" % (k, kwargs[k]) for k in kwargs])


class TestParametrized(Test):
    @parametrize(x=1, y=2)
    def test_param(self, x, y):
        return stringify_kwargs(x=x, y=y)

    @parametrize(x=10, y=20)
    @parametrize(x=1, y=2)
    def test_param2(self, x, y):
        return stringify_kwargs(x=x, y=y)

    @matrix(y=[-1, -2, -3])
    def test_matrix(self, x="default_x", y="default_y"):
        return stringify_kwargs(x=x, y=y)

    @matrix(x=[10, 20, 30])
    @matrix(y=[-1, -2, -3])
    def test_matrix2(self, x, y):
        return stringify_kwargs(x=x, y=y)