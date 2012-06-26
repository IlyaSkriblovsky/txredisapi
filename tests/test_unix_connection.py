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

import os

import txredisapi as redis

from twisted.internet import base
from twisted.internet import defer
from twisted.trial import unittest

base.DelayedCall.debug = False
redis_sock = "/tmp/redis.sock"


class TestUnixConnectionMethods(unittest.TestCase):
    @defer.inlineCallbacks
    def test_UnixConnection(self):
        db = yield redis.UnixConnection(redis_sock, reconnect=False)
        self.assertEqual(isinstance(db, redis.UnixConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_UnixConnectionDB1(self):
        db = yield redis.UnixConnection(redis_sock, dbid=1, reconnect=False)
        self.assertEqual(isinstance(db, redis.UnixConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_UnixConnectionPool(self):
        db = yield redis.UnixConnectionPool(redis_sock, poolsize=2,
                                            reconnect=False)
        self.assertEqual(isinstance(db, redis.UnixConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyUnixConnection(self):
        db = redis.lazyUnixConnection(redis_sock, reconnect=False)
        self.assertEqual(isinstance(db._connected, defer.Deferred), True)
        db = yield db._connected
        self.assertEqual(isinstance(db, redis.UnixConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyUnixConnectionPool(self):
        db = redis.lazyUnixConnectionPool(redis_sock, reconnect=False)
        self.assertEqual(isinstance(db._connected, defer.Deferred), True)
        db = yield db._connected
        self.assertEqual(isinstance(db, redis.UnixConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ShardedUnixConnection(self):
        paths = [redis_sock]
        db = yield redis.ShardedUnixConnection(paths, reconnect=False)
        self.assertEqual(isinstance(db,
                                    redis.ShardedUnixConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ShardedUnixConnectionPool(self):
        paths = [redis_sock]
        db = yield redis.ShardedUnixConnectionPool(paths, reconnect=False)
        self.assertEqual(isinstance(db,
                                    redis.ShardedUnixConnectionHandler), True)
        yield db.disconnect()

if not os.path.exists(redis_sock):
    TestUnixConnectionMethods.skip = True
