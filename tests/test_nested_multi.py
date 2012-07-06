# coding: utf-8
# Copyright 2009 Alexandre Fiori
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

import txredisapi as redis
from twisted.internet import defer
from twisted.python.failure import Failure
from twisted.trial import unittest

redis_host = "localhost"
redis_port = 6379


class TestNestedMulti(unittest.TestCase):
    _KEYS = ['txredisapi:testmulti1', 'txredisapi:testmulti2']

    @defer.inlineCallbacks
    def setUp(self):
        self.db = yield redis.Connection(redis_host, redis_port,
                                         reconnect=False)

    @defer.inlineCallbacks
    def tearDown(self):
        yield defer.gatherResults([self.db.delete(x) for x in self._KEYS])
        yield self.db.disconnect()

    @defer.inlineCallbacks
    def testRedisNestedMulti(self):
        t = yield self.db.multi()
        yield t.hmset(self._KEYS[0], {'foo': 'fooz', 'bar': 'baz'})
        yield t.hmset(self._KEYS[1], {'foo2': 'fooz2', 'bar2': 'baz2'})

        for key in self._KEYS:
            yield t.hgetall(key)
        result = yield t.commit()

        expected = [
            'OK',
            'OK',
            [u'foo', u'fooz', u'bar', u'baz'],
            [u'foo2', u'fooz2', u'bar2', u'baz2']
        ]
        for index, expect in enumerate(expected):
            self.assertEqual(set(result[index]), set(expect))
