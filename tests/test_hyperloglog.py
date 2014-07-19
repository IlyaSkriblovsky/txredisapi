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
from twisted.trial import unittest

from .mixins import RedisVersionCheckMixin, REDIS_HOST, REDIS_PORT


class TestHyperLogLog(unittest.TestCase, RedisVersionCheckMixin):
    _KEYS = ['_hll_test_key1', '_hll_test_key2',
             '_hll_test_key3']

    @defer.inlineCallbacks
    def setUp(self):
        self.db = yield redis.Connection(REDIS_HOST, REDIS_PORT,
                                         reconnect=False)
        self.redis_2_8_9 = yield self.checkVersion(2, 8, 9)
        yield self.db.delete(*self._KEYS)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.db.delete(*self._KEYS)
        yield self.db.disconnect()

    @defer.inlineCallbacks
    def test_pfadd(self):
        self._skipCheck()
        key = self._KEYS[0]
        r = yield self.db.pfadd(key, 'a', 'b', 'c', 'd', 'e', 'f', 'g')
        self.assertEqual(r, 1)
        cnt = yield self.db.pfcount(key)
        self.assertEqual(cnt, 7)

    @defer.inlineCallbacks
    def test_pfcount(self):
        self._skipCheck()
        key1 = self._KEYS[0]
        key2 = self._KEYS[1]
        r = yield self.db.pfadd(key1, 'foo', 'bar', 'zap')
        self.assertEqual(r, 1)
        r1 = yield self.db.pfadd(key1, 'zap', 'zap', 'zap')
        self.assertEqual(r1, 0)
        r2 = yield self.db.pfadd(key1, 'foo', 'bar')
        self.assertEqual(r2, 0)
        cnt1 = yield self.db.pfcount(key1)
        self.assertEqual(cnt1, 3)
        r3 = yield self.db.pfadd(key2, 1, 2, 3)
        self.assertEqual(r3, 1)
        cnt2 = yield self.db.pfcount(key1, key2)
        self.assertEqual(cnt2, 6)

    @defer.inlineCallbacks
    def test_pfmerge(self):
        self._skipCheck()
        key1 = self._KEYS[0]
        key2 = self._KEYS[1]
        key3 = self._KEYS[2]
        r = yield self.db.pfadd(key1, 'foo', 'bar', 'zap', 'a')
        self.assertEqual(r, 1)
        r1 = yield self.db.pfadd(key2, 'a', 'b', 'c', 'foo')
        self.assertEqual(r1, 1)
        yield self.db.pfmerge(key3, key1, key2)
        cnt = yield self.db.pfcount(key3)
        self.assertEqual(cnt, 6)

    def _skipCheck(self):
        if not self.redis_2_8_9:
            skipMsg = "Redis version < 2.8.9 (found version: %s)"
            raise unittest.SkipTest(skipMsg % self.redis_version)
