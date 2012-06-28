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
from twisted.trial import unittest

redis_host = "localhost"
redis_port = 6379


class TestRedisConnections(unittest.TestCase):
    @defer.inlineCallbacks
    def testRedisOperations(self):
        db = yield redis.Connection(redis_host, redis_port, reconnect=False)

        # test set() operation
        kvpairs = (("txredisapi:test1", "foo"), ("txredisapi:test2", "bar"))
        for key, value in kvpairs:
            yield db.set(key, value)
            result = yield db.get(key)
            self.assertEqual(result, value)

        d = {"txredisapi:a": 1, "txredisapi:b": 2}
        yield db.mset(d)
        values = yield db.mget(d.keys())
        self.assertEqual(values, d.values())

        keys = ['txredisapi:a', 'txredisapi:notset', 'txredisapi:b']
        values = yield db.mget(keys)
        self.assertEqual(values, [1, None, 2])

        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisError(self):
        db = yield redis.Connection(redis_host, redis_port, reconnect=False)
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
