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
        
        for hk in ("foo", "bar"):
            yield rapi.hset("txredisapi:HSetHGet", hk, 1)
            result = yield rapi.hget("txredisapi:HSetHGet", hk)
            self.assertEqual(result, 1)

        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testRedisHMSetHMGet(self):
        rapi = yield txredisapi.RedisConnection(redis_host, redis_port)
        t_dict = {}
        t_dict['key1'] = 'uno'
        t_dict['key2'] = 'dos'
        s = yield rapi.hmset("txredisapi:HMSetHMGet", t_dict)
        ks = t_dict.keys()
        ks.reverse()
        vs = t_dict.values()
        vs.reverse()
        res = yield rapi.hmget("txredisapi:HMSetHMGet", ks)
        self.assertEqual(vs, res)

        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testRedisHKeysHVals(self):
        rapi = yield txredisapi.RedisConnection(redis_host, redis_port)
        t_dict = {}
        t_dict['key1'] = 'uno'
        t_dict['key2'] = 'dos'
        s = yield rapi.hmset("txredisapi:HKeysHVals", t_dict)

        vs_u = [unicode(v) for v in t_dict.values()]
        ks_u = [unicode(k) for k in t_dict.keys()]
        k_res = yield rapi.hkeys("txredisapi:HKeysHVals")
        v_res = yield rapi.hvals("txredisapi:HKeysHVals")
        self.assertEqual(ks_u, k_res)
        self.assertEqual(vs_u, v_res)

        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testRedisHIncrBy(self):
        rapi = yield txredisapi.RedisConnection(redis_host, redis_port)
        yield rapi.hset("txredisapi:HIncrBy", "value", 1)
        yield rapi.hincr("txredisapi:HIncrBy", "value")
        yield rapi.hincrby("txredisapi:HIncrBy", "value", 2) 
        result = yield rapi.hget("txredisapi:HIncrBy", "value")
        self.assertEqual(result, 4)
        
        yield rapi.hincrby("txredisapi:HIncrBy", "value", 10)
        yield rapi.hdecr("txredisapi:HIncrBy", "value")
        result = yield rapi.hget("txredisapi:HIncrBy", "value")
        self.assertEqual(result, 13)
        
        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testRedisHLenHDelHExists(self):
        rapi = yield txredisapi.RedisConnection(redis_host, redis_port)
        t_dict = {}
        t_dict['key1'] = 'uno'
        t_dict['key2'] = 'dos'
        
        s = yield rapi.hmset("txredisapi:HDelHExists", t_dict)
        r_len = yield rapi.hlen("txredisapi:HDelHExists")
        self.assertEqual(r_len, 2)
        
        s = yield rapi.hdel("txredisapi:HDelHExists", "key2")
        r_len = yield rapi.hlen("txredisapi:HDelHExists")
        self.assertEqual(r_len, 1)
        
        s = yield rapi.hexists("txredisapi:HDelHExists", "key2")
        self.assertEqual(s, 0)
        
        yield rapi.disconnect()
        
    @defer.inlineCallbacks
    def testRedisHGetAll(self):
        rapi = yield txredisapi.RedisConnection(redis_host, redis_port)

        d = {u"key1":u"uno", u"key2":u"dos"}
        yield rapi.hmset("txredisapi:HGetAll", d)
        s = yield rapi.hgetall("txredisapi:HGetAll")

        self.assertEqual(d, s)
        yield rapi.disconnect() 
