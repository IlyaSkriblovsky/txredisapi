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

import txredisapi
from twisted.trial import unittest
from twisted.internet import base, defer, reactor

base.DelayedCall.debug = False
redis_host="localhost"
redis_port=6379

class TestRedisConnectionMethods(unittest.TestCase):
    @defer.inlineCallbacks
    def test_RedisConnection(self):
        # RedisConnection returns deferred, which gets RedisAPI
        conn = txredisapi.RedisConnection(redis_host, redis_port)
        self.assertEqual(isinstance(conn, defer.Deferred), True)
        rapi = yield conn
        self.assertEqual(isinstance(rapi, txredisapi.RedisAPI), True)
        disconnected = yield rapi.disconnect()
        self.assertEqual(disconnected, True)
        
    @defer.inlineCallbacks
    def test_RedisConnectionPool(self):
        # RedisConnectionPool returns deferred, which gets RedisAPI
        conn = txredisapi.RedisConnectionPool(redis_host, redis_port, pool_size=2)
        self.assertEqual(isinstance(conn, defer.Deferred), True)
        rapi = yield conn
        self.assertEqual(isinstance(rapi, txredisapi.RedisAPI), True)
        disconnected = yield rapi.disconnect()
        self.assertEqual(disconnected, True)

    @defer.inlineCallbacks
    def test_lazyRedisConnection(self):
        # lazyRedisConnection returns RedisAPI
        rapi = txredisapi.lazyRedisConnection(redis_host, redis_port)
        self.assertEqual(isinstance(rapi, txredisapi.RedisAPI), True)
        yield rapi._connected
        disconnected = yield rapi.disconnect()
        self.assertEqual(disconnected, True)

    @defer.inlineCallbacks
    def test_lazyRedisConnectionPool(self):
        # lazyRedisConnection returns RedisAPI
        rapi = txredisapi.lazyRedisConnectionPool(redis_host, redis_port, pool_size=2)
        self.assertEqual(isinstance(rapi, txredisapi.RedisAPI), True)
        yield rapi._connected
        disconnected = yield rapi.disconnect()
        self.assertEqual(disconnected, True)
