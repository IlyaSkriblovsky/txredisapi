# coding: utf-8
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

from twisted.internet import defer
from twisted.trial import unittest

import txredisapi as redis

from tests.mixins import REDIS_HOST, REDIS_PORT


class TestRedisSort(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.db = yield redis.Connection(REDIS_HOST, REDIS_PORT,
                                         reconnect=False)
        yield self.db.delete('txredisapi:values')
        yield self.db.lpush('txredisapi:values', [5, 3, 19, 2, 4, 34, 12])

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.db.disconnect()

    @defer.inlineCallbacks
    def testSort(self):
        r = yield self.db.sort('txredisapi:values')
        self.assertEqual([2, 3, 4, 5, 12, 19, 34], r)

    @defer.inlineCallbacks
    def testSortWithEndOnly(self):
        try:
            yield self.db.sort('txredisapi:values', end=3)
        except redis.RedisError:
            pass
        else:
            self.fail('RedisError not raised: no start parameter given')

    @defer.inlineCallbacks
    def testSortWithStartOnly(self):
        try:
            yield self.db.sort('txredisapi:values', start=3)
        except redis.RedisError:
            pass
        else:
            self.fail('RedisError not raised: no end parameter given')

    @defer.inlineCallbacks
    def testSortWithLimits(self):
        r = yield self.db.sort('txredisapi:values', start=2, end=4)
        self.assertEqual([4, 5, 12, 19], r)

    @defer.inlineCallbacks
    def testSortAlpha(self):
        yield self.db.delete('txredisapi:alphavals')
        yield self.db.lpush('txredisapi:alphavals', ['dog', 'cat', 'apple'])

        r = yield self.db.sort('txredisapi:alphavals', alpha=True)
        self.assertEquals(['apple', 'cat', 'dog'], r)
