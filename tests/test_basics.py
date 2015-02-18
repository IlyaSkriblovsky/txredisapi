# coding: utf-8
# Copyright 2015 Jeethu Rao
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

from .mixins import REDIS_HOST, REDIS_PORT


class TestBasics(unittest.TestCase):
    _KEYS = ['_test_key1', '_test_key2', '_test_key3']
    skipTeardown = False

    @defer.inlineCallbacks
    def setUp(self):
        self.skipTeardown = False
        self.db = yield redis.Connection(REDIS_HOST, REDIS_PORT,
                                         reconnect=False)

    @defer.inlineCallbacks
    def tearDown(self):
        if not self.skipTeardown:
            yield self.db.delete(*self._KEYS)
            yield self.db.disconnect()

    def test_quit(self):
        self.skipTeardown = True
        return self.db.quit()

    def test_auth(self):
        self.assertFailure(self.db.auth('test'),
                           redis.ResponseError)

    @defer.inlineCallbacks
    def test_ping(self):
        r = yield self.db.ping()
        self.assertEqual(r, u'PONG')

    @defer.inlineCallbacks
    def test_exists(self):
        yield self.db.delete(self._KEYS[0])
        r = yield self.db.exists(self._KEYS[0])
        self.assertFalse(r)
        yield self.db.set(self._KEYS[0], 1)
        r = yield self.db.exists(self._KEYS[0])
        self.assertTrue(r)

    @defer.inlineCallbacks
    def test_type(self):
        yield self.db.set(self._KEYS[0], "value")
        yield self.db.lpush(self._KEYS[1], 1)
        yield self.db.sadd(self._KEYS[2], 1)
        r = yield self.db.type(self._KEYS[0])
        self.assertEqual(r, "string")
        r = yield self.db.type(self._KEYS[1])
        self.assertEqual(r, "list")
        r = yield self.db.type(self._KEYS[2])
        self.assertEqual(r, "set")

    @defer.inlineCallbacks
    def test_keys(self):
        yield self.db.set(self._KEYS[0], "value")
        yield self.db.set(self._KEYS[1], "value")
        r = yield self.db.keys('_test_key*')
        self.assertIn(self._KEYS[0], r)
        self.assertIn(self._KEYS[1], r)

    @defer.inlineCallbacks
    def test_randomkey(self):
        yield self.db.set(self._KEYS[0], "value")
        r = yield self.db.randomkey()
        assert r is not None

    @defer.inlineCallbacks
    def test_rename(self):
        yield self.db.set(self._KEYS[0], "value")
        yield self.db.delete(self._KEYS[1])
        r = yield self.db.rename(self._KEYS[0], self._KEYS[1])
        self.assertTrue(r)
        r = yield self.db.get(self._KEYS[1])
        self.assertEqual(r, "value")
        r = yield self.db.exists(self._KEYS[0])
        self.assertFalse(r)

    @defer.inlineCallbacks
    def test_renamenx(self):
        yield self.db.set(self._KEYS[0], "value")
        yield self.db.set(self._KEYS[1], "value1")
        r = yield self.db.renamenx(self._KEYS[0], self._KEYS[1])
        self.assertFalse(r)
        r = yield self.db.renamenx(self._KEYS[1], self._KEYS[2])
        self.assertTrue(r)
        r = yield self.db.get(self._KEYS[2])
        self.assertEqual(r, "value1")

    @defer.inlineCallbacks
    def test_dbsize(self):
        yield self.db.set(self._KEYS[0], "value")
        yield self.db.set(self._KEYS[1], "value1")
        r = yield self.db.dbsize()
        assert r >= 2
