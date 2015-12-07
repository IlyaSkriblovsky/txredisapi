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

import random

from twisted.internet import defer
from twisted.trial import unittest

import txredisapi as redis

from tests.mixins import REDIS_HOST, REDIS_PORT


class SetsTests(unittest.TestCase):
    '''
    Tests to ensure that set returning operations return sets
    '''
    _KEYS = ['txredisapi:testsets1', 'txredisapi:testsets2',
             'txredisapi:testsets3', 'txredisapi:testsets4']
    N = 1024

    @defer.inlineCallbacks
    def setUp(self):
        self.db = yield redis.Connection(REDIS_HOST, REDIS_PORT,
                                         reconnect=False)

    @defer.inlineCallbacks
    def tearDown(self):
        yield defer.gatherResults([self.db.delete(x) for x in self._KEYS])
        yield self.db.disconnect()

    @defer.inlineCallbacks
    def test_saddrem(self):
        s = set(range(self.N))
        r = yield self.db.sadd(self._KEYS[0], s)
        self.assertEqual(r, len(s))
        a = s.pop()
        r = yield self.db.srem(self._KEYS[0], a)
        self.assertEqual(r, 1)
        l = [s.pop() for x in range(self.N >> 2)]
        r = yield self.db.srem(self._KEYS[0], l)
        self.assertEqual(r, len(l))
        r = yield self.db.srem(self._KEYS[0], self.N + 1)
        self.assertEqual(r, 0)
        r = yield self.db.smembers(self._KEYS[0])
        self.assertIsInstance(r, set)
        self.assertEqual(r, s)

    @defer.inlineCallbacks
    def _test_set(self, key, s):
        '''
        Check if the Redis set and the Python set are identical
        '''
        r = yield self.db.scard(key)
        self.assertEqual(r, len(s))
        r = yield self.db.smembers(key)
        self.assertEqual(r, s)

    @defer.inlineCallbacks
    def test_sunion(self):
        s = set(range(self.N))
        s1 = set()
        for x in range(4):
            ss = set(s.pop() for x in range(self.N >> 2))
            s1.update(ss)
            r = yield self.db.sadd(self._KEYS[x], ss)
            self.assertEqual(r, len(ss))
        r = yield self.db.sunion(self._KEYS[:4])
        self.assertIsInstance(r, set)
        self.assertEqual(r, s1)
        # Test sunionstore
        r = yield self.db.sunionstore(self._KEYS[0], self._KEYS[:4])
        self.assertEqual(r, len(s1))
        yield self._test_set(self._KEYS[0], s1)

    @defer.inlineCallbacks
    def test_sdiff(self):
        l = list(range(self.N))
        random.shuffle(l)
        p1 = set(l[:self.N >> 1])
        random.shuffle(l)
        p2 = set(l[:self.N >> 1])
        r = yield self.db.sadd(self._KEYS[0], p1)
        self.assertEqual(r, len(p1))
        r = yield self.db.sadd(self._KEYS[1], p2)
        self.assertEqual(r, len(p2))
        r = yield self.db.sdiff(self._KEYS[:2])
        self.assertIsInstance(r, set)
        a = p1 - p2
        self.assertEqual(r, a)
        # Test sdiffstore
        r = yield self.db.sdiffstore(self._KEYS[0], self._KEYS[:2])
        self.assertEqual(r, len(a))
        yield self._test_set(self._KEYS[0], a)

    @defer.inlineCallbacks
    def test_sinter(self):
        l = list(range(self.N))
        random.shuffle(l)
        p1 = set(l[:self.N >> 1])
        random.shuffle(l)
        p2 = set(l[:self.N >> 1])
        r = yield self.db.sadd(self._KEYS[0], p1)
        self.assertEqual(r, len(p1))
        r = yield self.db.sadd(self._KEYS[1], p2)
        self.assertEqual(r, len(p2))
        r = yield self.db.sinter(self._KEYS[:2])
        self.assertIsInstance(r, set)
        a = p1.intersection(p2)
        self.assertEqual(r, a)
        # Test sinterstore
        r = yield self.db.sinterstore(self._KEYS[0], self._KEYS[:2])
        self.assertEqual(r, len(a))
        yield self._test_set(self._KEYS[0], a)

    @defer.inlineCallbacks
    def test_smembers(self):
        s = set(range(self.N))
        r = yield self.db.sadd(self._KEYS[0], s)
        self.assertEqual(r, len(s))
        r = yield self.db.smembers(self._KEYS[0])
        self.assertIsInstance(r, set)
        self.assertEqual(r, s)

    @defer.inlineCallbacks
    def test_sismemember(self):
        yield self.db.sadd(self._KEYS[0], 1)
        r = yield self.db.sismember(self._KEYS[0], 1)
        self.assertIsInstance(r, bool)
        self.assertEqual(r, True)
        yield self.db.srem(self._KEYS[0], 1)
        r = yield self.db.sismember(self._KEYS[0], 1)
        self.assertIsInstance(r, bool)
        self.assertEqual(r, False)

    @defer.inlineCallbacks
    def test_smove(self):
        yield self.db.sadd(self._KEYS[0], [1, 2, 3])
        # Test moving an existing element
        r = yield self.db.smove(self._KEYS[0], self._KEYS[1], 1)
        self.assertIsInstance(r, bool)
        self.assertEqual(r, True)
        r = yield self.db.smembers(self._KEYS[1])
        self.assertEqual(r, set([1]))
        # Test moving an non existing element
        r = yield self.db.smove(self._KEYS[0], self._KEYS[1], 4)
        self.assertIsInstance(r, bool)
        self.assertEqual(r, False)
        r = yield self.db.smembers(self._KEYS[1])
        self.assertEqual(r, set([1]))

    @defer.inlineCallbacks
    def test_srandmember(self):
        l = range(10)
        yield self.db.sadd(self._KEYS[0], l)
        for i in l:
            r = yield self.db.srandmember(self._KEYS[0])
            self.assertIn(r, l)

    @defer.inlineCallbacks
    def test_spop(self):
        l = range(10)
        yield self.db.sadd(self._KEYS[0], l)
        popped = set()
        for i in l:
            r = yield self.db.spop(self._KEYS[0])
            self.assertNotIn(r, popped)
            popped.add(r)
        self.assertEqual(set(l), popped)
