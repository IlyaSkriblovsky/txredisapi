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
from twisted.internet import defer
from twisted.trial import unittest

redis_host = "localhost"
redis_port = 6379


class LargeMultiBulk(unittest.TestCase):
    _KEY = 'txredisapi:testlargemultibulk'

    @defer.inlineCallbacks
    def setUp(self):
        self.db = yield redis.Connection(
                redis_host, redis_port, reconnect=False)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.db.delete(self._KEY)
        yield self.db.disconnect()

    @defer.inlineCallbacks
    def _test_multibulk(self, data):
        yield defer.DeferredList([self.db.sadd(self._KEY, x) for x in data])
        res = yield self.db.smembers(self._KEY)
        self.assertEqual(set(res), data)

    def test_large_multibulk_int(self):
        data = set(range(1000))
        return self._test_multibulk(data)

    def test_large_multibulk_str(self):
        data = set([os.urandom(10).encode('base64') for x in range(100)])
        return self._test_multibulk(data)
