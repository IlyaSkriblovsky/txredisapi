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

from twisted.internet import base
from twisted.internet import defer
from twisted.internet import reactor
from twisted.trial import unittest

base.DelayedCall.debug = False
redis_host="localhost"
redis_port=6379

class TestConnectionMethods(unittest.TestCase):
    @defer.inlineCallbacks
    def test_Connection(self):
        db = yield redis.Connection(redis_host, redis_port, reconnect=False)
        self.assertEqual(isinstance(db, redis.ConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ConnectionDB1(self):
        db = yield redis.Connection(redis_host, redis_port, dbid=1, reconnect=False)
        self.assertEqual(isinstance(db, redis.ConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ConnectionPool(self):
        db = yield redis.ConnectionPool(redis_host, redis_port, poolsize=2, reconnect=False)
        self.assertEqual(isinstance(db, redis.ConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyConnection(self):
        db = redis.lazyConnection(redis_host, redis_port, reconnect=False)
        self.assertEqual(isinstance(db._connected, defer.Deferred), True)
        db = yield db._connected
        self.assertEqual(isinstance(db, redis.ConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyConnectionPool(self):
        db = redis.lazyConnectionPool(redis_host, redis_port, reconnect=False)
        self.assertEqual(isinstance(db._connected, defer.Deferred), True)
        db = yield db._connected
        self.assertEqual(isinstance(db, redis.ConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ShardedConnection(self):
        hosts = ["%s:%s" % (redis_host, redis_port)]
        db = yield redis.ShardedConnection(hosts, reconnect=False)
        self.assertEqual(isinstance(db, redis.ShardedConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ShardedConnectionPool(self):
        hosts = ["%s:%s" % (redis_host, redis_port)]
        db = yield redis.ShardedConnectionPool(hosts, reconnect=False)
        self.assertEqual(isinstance(db, redis.ShardedConnectionHandler), True)
        yield db.disconnect()
