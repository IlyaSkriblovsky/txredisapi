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
from twisted.internet import reactor
from twisted.trial import unittest

redis_host="localhost"
redis_port=6379

class TestRedisHashOperations(unittest.TestCase):
    @defer.inlineCallbacks
    def testRedisHSetHGet(self):
        db = yield redis.Connection(redis_host, redis_port, reconnect=False)
        for hk in ("foo", "bar"):
            yield db.hset("txredisapi:HSetHGet", hk, 1)
            result = yield db.hget("txredisapi:HSetHGet", hk)
            self.assertEqual(result, 1)

        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisHMSetHMGet(self):
        db = yield redis.Connection(redis_host, redis_port, reconnect=False)
        t_dict = {}
        t_dict['key1'] = 'uno'
        t_dict['key2'] = 'dos'
        s = yield db.hmset("txredisapi:HMSetHMGet", t_dict)
        ks = t_dict.keys()
        ks.reverse()
        vs = t_dict.values()
        vs.reverse()
        res = yield db.hmget("txredisapi:HMSetHMGet", ks)
        self.assertEqual(vs, res)

        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisHKeysHVals(self):
        db = yield redis.Connection(redis_host, redis_port, reconnect=False)
        t_dict = {}
        t_dict['key1'] = 'uno'
        t_dict['key2'] = 'dos'
        s = yield db.hmset("txredisapi:HKeysHVals", t_dict)

        vs_u = [unicode(v) for v in t_dict.values()]
        ks_u = [unicode(k) for k in t_dict.keys()]
        k_res = yield db.hkeys("txredisapi:HKeysHVals")
        v_res = yield db.hvals("txredisapi:HKeysHVals")
        self.assertEqual(ks_u, k_res)
        self.assertEqual(vs_u, v_res)

        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisHIncrBy(self):
        db = yield redis.Connection(redis_host, redis_port, reconnect=False)
        yield db.hset("txredisapi:HIncrBy", "value", 1)
        yield db.hincr("txredisapi:HIncrBy", "value")
        yield db.hincrby("txredisapi:HIncrBy", "value", 2)
        result = yield db.hget("txredisapi:HIncrBy", "value")
        self.assertEqual(result, 4)

        yield db.hincrby("txredisapi:HIncrBy", "value", 10)
        yield db.hdecr("txredisapi:HIncrBy", "value")
        result = yield db.hget("txredisapi:HIncrBy", "value")
        self.assertEqual(result, 13)

        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisHLenHDelHExists(self):
        db = yield redis.Connection(redis_host, redis_port, reconnect=False)
        t_dict = {}
        t_dict['key1'] = 'uno'
        t_dict['key2'] = 'dos'

        s = yield db.hmset("txredisapi:HDelHExists", t_dict)
        r_len = yield db.hlen("txredisapi:HDelHExists")
        self.assertEqual(r_len, 2)

        s = yield db.hdel("txredisapi:HDelHExists", "key2")
        r_len = yield db.hlen("txredisapi:HDelHExists")
        self.assertEqual(r_len, 1)

        s = yield db.hexists("txredisapi:HDelHExists", "key2")
        self.assertEqual(s, 0)

        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisHGetAll(self):
        db = yield redis.Connection(redis_host, redis_port, reconnect=False)

        d = {u"key1":u"uno", u"key2":u"dos"}
        yield db.hmset("txredisapi:HGetAll", d)
        s = yield db.hgetall("txredisapi:HGetAll")

        self.assertEqual(d, s)
        yield db.disconnect()
