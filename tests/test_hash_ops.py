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
from twisted.internet import defer, reactor

redis_host="localhost"
redis_port=6379

class TestRedisHashOperations(unittest.TestCase):
    @defer.inlineCallbacks
    def testRedisHSetHGet(self):
        rapi = yield txredisapi.RedisConnection(redis_host, redis_port)
        
        for value in ("foo", "bar"):
            c = yield rapi.hset("test", value, 1)
            result = yield rapi.hget("test", value)
            self.assertEqual(result, 1)

        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testRedisHMSetHMGet(self):
        rapi = yield txredisapi.RedisConnection(redis_host, redis_port)
        t_dict = {}
        t_dict['key1'] = 'uno'
        t_dict['key2'] = 'dos'
        s = yield rapi.hmset("testmh", t_dict)
        ks = t_dict.keys()
        ks.reverse()
        vs = t_dict.values()
        vs.reverse()
        res = yield rapi.hmget("testmh", ks)
        self.assertEqual(vs, res)

        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testRedisHKeysHVals(self):
        rapi = yield txredisapi.RedisConnection(redis_host, redis_port)
        t_dict = {}
        t_dict['key1'] = 'uno'
        t_dict['key2'] = 'dos'
        s = yield rapi.hmset("testkv", t_dict)

        vs_u = [unicode(v) for v in t_dict.values()]
        ks_u = [unicode(k) for k in t_dict.keys()]
        k_res = yield rapi.hkeys("testkv")
        v_res = yield rapi.hvals("testkv")
        self.assertEqual(ks_u, k_res)
        self.assertEqual(vs_u, v_res)

        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testRedisHIncrBy(self):
        rapi = yield txredisapi.RedisConnection(redis_host, redis_port)
        c = yield rapi.hset("test_incr", "value", 1)
        c = yield rapi.hincrby("test_incr", "value")
        result = yield rapi.hget("test_incr", "value")
        self.assertEqual(result, 2)
        
        c = yield rapi.hincrby("test_incr", "value", 10)
        result = yield rapi.hget("test_incr", "value")
        self.assertEqual(result, 12)
        
        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testRedisHLenHDelHExists(self):
        rapi = yield txredisapi.RedisConnection(redis_host, redis_port)
        t_dict = {}
        t_dict['key1'] = 'uno'
        t_dict['key2'] = 'dos'
        
        s = yield rapi.hmset("testld", t_dict)
        r_len = yield rapi.hlen("testld")
        self.assertEqual(r_len, 2)
        
        s = yield rapi.hdel("testld", "key2")
        r_len = yield rapi.hlen("testld")
        self.assertEqual(r_len, 1)
        
        s = yield rapi.hexists("testld", "key2")
        self.assertEqual(s, 0)
        
        yield rapi.disconnect()
        
    @defer.inlineCallbacks
    def testRedisHGetAll(self):
        rapi = yield txredisapi.RedisConnection(redis_host, redis_port)
        t_dict = {}
        t_dict['key1'] = 'uno'
        t_dict['key2'] = 'dos'
        
        s = yield rapi.hmset("testga", t_dict)
        s = yield rapi.hgetall("testga")

        all_u = []
        [all_u.extend(p) for p in t_dict.iteritems()]
        self.assertEqual(s, all_u)
        yield rapi.disconnect() 
        
        
