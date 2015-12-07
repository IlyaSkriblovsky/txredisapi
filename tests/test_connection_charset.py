# coding: utf-8
# Copyright 2013 Ilia Glazkov
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

import six

from twisted.internet import defer
from twisted.trial import unittest

import txredisapi as redis

from tests.mixins import REDIS_HOST, REDIS_PORT


class TestConnectionCharset(unittest.TestCase):
    TEST_KEY = 'txredisapi:test_key'
    TEST_VALUE_UNICODE = six.text_type('\u262d' * 3)
    TEST_VALUE_BINARY = b'\x00\x01' * 3

    @defer.inlineCallbacks
    def test_charset_None(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, charset=None)

        yield db.set(self.TEST_KEY, self.TEST_VALUE_BINARY)
        result = yield db.get(self.TEST_KEY)
        self.assertTrue(type(result) == six.binary_type)
        self.assertEqual(result, self.TEST_VALUE_BINARY)

        yield db.delete(self.TEST_KEY)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_charset_default(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT)

        yield db.set(self.TEST_KEY, self.TEST_VALUE_UNICODE)
        result = yield db.get(self.TEST_KEY)
        self.assertEqual(result, self.TEST_VALUE_UNICODE)
        self.assertTrue(type(result) == six.text_type)

        yield db.delete(self.TEST_KEY)
        yield db.disconnect()
