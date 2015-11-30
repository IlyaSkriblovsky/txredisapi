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

from twisted.internet import defer
from twisted.python.failure import Failure
from twisted.trial import unittest

import txredisapi as redis

from tests.mixins import REDIS_HOST, REDIS_PORT


class TestRedisConnections(unittest.TestCase):
    _KEYS = ['txredisapi:testwatch1', 'txredisapi:testwatch2']

    @defer.inlineCallbacks
    def setUp(self):
        self.connections = []
        self.db = yield self._getRedisConnection()
        yield self.db.delete(self._KEYS)

    @defer.inlineCallbacks
    def tearDown(self):
        for connection in self.connections:
            l = [connection.delete(k) for k in self._KEYS]
            yield defer.DeferredList(l)
            yield connection.disconnect()

    def _db_connected(self, connection):
        self.connections.append(connection)
        return connection

    def _getRedisConnection(self, host=REDIS_HOST, port=REDIS_PORT, db=0):
        return redis.Connection(
            host, port, dbid=db, reconnect=False).addCallback(
                self._db_connected)

    def _check_watcherror(self, response, shouldError=False):
        if shouldError:
            self.assertIsInstance(response, Failure)
            self.assertIsInstance(response.value, redis.WatchError)
        else:
            self.assertNotIsInstance(response, Failure)

    @defer.inlineCallbacks
    def testRedisWatchFail(self):
        db1 = yield self._getRedisConnection()
        yield self.db.set(self._KEYS[0], 'foo')
        t = yield self.db.multi(self._KEYS[0])
        self.assertIsInstance(t, redis.RedisProtocol)
        yield t.set(self._KEYS[1], 'bar')
        # This should trigger a failure
        yield db1.set(self._KEYS[0], 'bar1')
        yield t.commit().addBoth(self._check_watcherror, shouldError=True)

    @defer.inlineCallbacks
    def testRedisWatchSucceed(self):
        yield self.db.set(self._KEYS[0], 'foo')
        t = yield self.db.multi(self._KEYS[0])
        self.assertIsInstance(t, redis.RedisProtocol)
        yield t.set(self._KEYS[0], 'bar')
        yield t.commit().addBoth(self._check_watcherror, shouldError=False)

    @defer.inlineCallbacks
    def testRedisMultiNoArgs(self):
        yield self.db.set(self._KEYS[0], 'foo')
        t = yield self.db.multi()
        self.assertIsInstance(t, redis.RedisProtocol)
        yield t.set(self._KEYS[1], 'bar')
        yield t.commit().addBoth(self._check_watcherror, shouldError=False)

    @defer.inlineCallbacks
    def testRedisWithBulkCommands_transactions(self):
        t = yield self.db.watch(self._KEYS)
        yield t.mget(self._KEYS)
        t = yield t.multi()
        yield t.commit()
        self.assertEqual(0, t.transactions)
        self.assertFalse(t.inTransaction)

    @defer.inlineCallbacks
    def testRedisWithBulkCommands_inTransaction(self):
        t = yield self.db.watch(self._KEYS)
        yield t.mget(self._KEYS)
        self.assertTrue(t.inTransaction)
        yield t.unwatch()

    @defer.inlineCallbacks
    def testRedisWithBulkCommands_mget(self):
        yield self.db.set(self._KEYS[0], "foo")
        yield self.db.set(self._KEYS[1], "bar")

        m0 = yield self.db.mget(self._KEYS)
        t = yield self.db.watch(self._KEYS)
        m1 = yield t.mget(self._KEYS)
        t = yield t.multi()
        yield t.mget(self._KEYS)
        (m2,) = yield t.commit()

        self.assertEqual(["foo", "bar"], m0)
        self.assertEqual(m0, m1)
        self.assertEqual(m0, m2)

    @defer.inlineCallbacks
    def testRedisWithBulkCommands_hgetall(self):
        yield self.db.hset(self._KEYS[0], "foo", "bar")
        yield self.db.hset(self._KEYS[0], "bar", "foo")

        h0 = yield self.db.hgetall(self._KEYS[0])
        t = yield self.db.watch(self._KEYS[0])
        h1 = yield t.hgetall(self._KEYS[0])
        t = yield t.multi()
        yield t.hgetall(self._KEYS[0])
        (h2,) = yield t.commit()

        self.assertEqual({"foo": "bar",
                          "bar": "foo"}, h0)
        self.assertEqual(h0, h1)
        self.assertEqual(h0, h2)

    @defer.inlineCallbacks
    def testRedisWithAsyncCommandsDuringWatch(self):
        yield self.db.hset(self._KEYS[0], "foo", "bar")
        yield self.db.hset(self._KEYS[0], "bar", "foo")

        h0 = yield self.db.hgetall(self._KEYS[0])
        t = yield self.db.watch(self._KEYS[0])
        (h1, h2) = yield defer.gatherResults([
            t.hgetall(self._KEYS[0]),
            t.hgetall(self._KEYS[0]),
        ], consumeErrors=True)
        yield t.unwatch()

        self.assertEqual({"foo": "bar",
                          "bar": "foo"}, h0)
        self.assertEqual(h0, h1)
        self.assertEqual(h0, h2)
