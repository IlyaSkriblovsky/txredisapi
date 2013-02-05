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


class TestRedisConnections(unittest.TestCase):
    _KEYS = ['txredisapi:testwatch1', 'txredisapi:testwatch2']

    @defer.inlineCallbacks
    def setUp(self):
        self.connections = []
        self.db = yield self._getRedisConnection()

    @defer.inlineCallbacks
    def tearDown(self):
        for connection in self.connections:
            l = [connection.delete(k) for k in self._KEYS]
            yield defer.DeferredList(l)
            yield connection.disconnect()

    def _db_connected(self, connection):
        self.connections.append(connection)
        return connection

    def _getRedisConnection(self, host=redis_host, port=redis_port, db=0):
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
    def testRedisWithBulkCommands(self):
        t = yield self.db.watch("foobar")
        yield t.mget(["foo", "bar"])
        t = yield t.multi()
        yield t.commit()
        self.assertEqual(0, t.transactions)
        self.assertFalse(t.inTransaction)
