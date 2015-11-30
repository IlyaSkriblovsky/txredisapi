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

import six

import txredisapi as redis
from twisted.internet import defer
from twisted.trial import unittest

from .mixins import REDIS_HOST, REDIS_PORT


class TestNumberConversions(unittest.TestCase):
    CONVERT_NUMBERS = True

    TEST_KEY = 'txredisapi:test_key'

    TEST_VECTORS = [
        # Integers
        (u'100', 100),
        (100, 100),
        (u'0', 0),
        (0, 0),

        # Floats
        (u'.1', 0.1),
        (0.1, 0.1),

        # +inf and -inf aren't handled by automatic conversions
        # test this behavior anyway for the sake of completeness.
        (u'+inf', u'+inf'),
        (u'-inf', u'-inf'),
        (u'NaN', u'NaN')
    ]

    @defer.inlineCallbacks
    def test_number_conversions(self):
        for k, v in self.TEST_VECTORS:
            yield self.db.set(self.TEST_KEY, k)
            result = yield self.db.get(self.TEST_KEY)
            if self.CONVERT_NUMBERS:
                self.assertIsInstance(result, type(v))
                self.assertEqual(result, v)
            else:
                if isinstance(k, float):
                    expected = format(k, "f")
                else:
                    expected = str(k)
                self.assertIsInstance(result, six.string_types)
                self.assertEqual(result, expected)

    @defer.inlineCallbacks
    def setUp(self):
        self.db = yield redis.Connection(REDIS_HOST, REDIS_PORT,
                                         convertNumbers=self.CONVERT_NUMBERS)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.db.delete(self.TEST_KEY)
        yield self.db.disconnect()


class TestNoNumberConversions(TestNumberConversions):
    CONVERT_NUMBERS = False

    @defer.inlineCallbacks
    def test_hashes(self):
        d = {
            'a': 1,
            'b': '2'
        }
        expected = {
            'a': '1',
            'b': '2'
        }
        yield self.db.hmset(self.TEST_KEY, d)
        r = yield self.db.hgetall(self.TEST_KEY)
        self.assertEqual(r, expected)
