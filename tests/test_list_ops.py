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
from twisted.trial import unittest

import txredisapi as redis

from tests.mixins import REDIS_HOST, REDIS_PORT


class TestRedisListOperations(unittest.TestCase):
    @defer.inlineCallbacks
    def testRedisLPUSHSingleValue(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield db.delete("txredisapi:LPUSH")
        yield db.lpush("txredisapi:LPUSH", "singlevalue")
        result = yield db.lpop("txredisapi:LPUSH")
        self.assertEqual(result, "singlevalue")
        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisLPUSHListOfValues(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield db.delete("txredisapi:LPUSH")
        yield db.lpush("txredisapi:LPUSH", [1, 2, 3])
        result = yield db.lrange("txredisapi:LPUSH", 0, -1)
        self.assertEqual(result, [3, 2, 1])
        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisRPUSHSingleValue(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield db.delete("txredisapi:RPUSH")
        yield db.lpush("txredisapi:RPUSH", "singlevalue")
        result = yield db.lpop("txredisapi:RPUSH")
        self.assertEqual(result, "singlevalue")
        yield db.disconnect()

    @defer.inlineCallbacks
    def testRedisRPUSHListOfValues(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield db.delete("txredisapi:RPUSH")
        yield db.lpush("txredisapi:RPUSH", [1, 2, 3])
        result = yield db.lrange("txredisapi:RPUSH", 0, -1)
        self.assertEqual(result, [3, 2, 1])
        yield db.disconnect()
