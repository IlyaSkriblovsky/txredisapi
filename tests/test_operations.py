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

from twisted.internet import defer, reactor
from twisted.trial import unittest

import txredisapi as redis

from tests.mixins import REDIS_HOST, REDIS_PORT


class TestRedisConnections(unittest.TestCase):
    @defer.inlineCallbacks
    def testRedisOperations1(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)

        # test set() operation
        kvpairs = (("txredisapi:test1", "foo"), ("txredisapi:test2", "bar"))
        for key, value in kvpairs:
            yield db.set(key, value)
            result = yield db.get(key)
            self.assertEqual(result, value)

        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisOperations2(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)

        k = ["txredisapi:a", "txredisapi:b"]
        v = [1, 2]
        yield db.mset(dict(zip(k, v)))
        values = yield db.mget(k)
        self.assertEqual(values, v)

        k = ['txredisapi:a', 'txredisapi:notset', 'txredisapi:b']
        values = yield db.mget(k)
        self.assertEqual(values, [1, None, 2])

        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisError(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield db.set('txredisapi:a', 'test')
        try:
            yield db.sort('txredisapi:a', end='a')
        except redis.RedisError:
            pass
        else:
            yield db.disconnect()
            self.fail('RedisError not raised')

        try:
            yield db.incr('txredisapi:a')
        except redis.ResponseError:
            pass
        else:
            yield db.disconnect()
            self.fail('ResponseError not raised on redis error')
        yield db.disconnect()
        try:
            yield db.get('txredisapi:a')
        except redis.ConnectionError:
            pass
        else:
            self.fail('ConnectionError not raised')

    @defer.inlineCallbacks
    def testRedisOperationsSet1(self):

        def sleep(secs):
            d = defer.Deferred()
            reactor.callLater(secs, d.callback, None)
            return d
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        key, value = "txredisapi:test1", "foo"
        # test expiration in milliseconds
        yield db.set(key, value, pexpire=10)
        result_1 = yield db.get(key)
        self.assertEqual(result_1, value)
        yield sleep(0.015)
        result_2 = yield db.get(key)
        self.assertEqual(result_2, None)

        # same thing but timeout in seconds
        yield db.set(key, value, expire=1)
        result_3 = yield db.get(key)
        self.assertEqual(result_3, value)
        yield sleep(1.001)
        result_4 = yield db.get(key)
        self.assertEqual(result_4, None)
        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisOperationsSet2(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        key, value = "txredisapi:test_exists", "foo"
        # ensure value does not exits and new value sets
        yield db.delete(key)
        yield db.set(key, value, only_if_not_exists=True)
        result_1 = yield db.get(key)
        self.assertEqual(result_1, value)

        # new values not set cos, values exists
        yield db.set(key, "foo2", only_if_not_exists=True)
        result_2 = yield db.get(key)
        # nothing changed result is same "foo"
        self.assertEqual(result_2, value)
        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisOperationsSet3(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        key, value = "txredisapi:test_not_exists", "foo_not_exists"
        # ensure that such key does not exits, and value not sets
        yield db.delete(key)
        yield db.set(key, value, only_if_exists=True)
        result_1 = yield db.get(key)
        self.assertEqual(result_1, None)

        # ensure key exits, and value updates
        yield db.set(key, value)
        yield db.set(key, "foo", only_if_exists=True)
        result_2 = yield db.get(key)
        self.assertEqual(result_2, "foo")
        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisOperationTime(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)

        time = yield db.time()
        self.assertIsInstance(time, list)
        self.assertEqual(len(time), 2)
        self.assertIsInstance(time[0], int)
        self.assertIsInstance(time[1], int)

        yield db.disconnect()
