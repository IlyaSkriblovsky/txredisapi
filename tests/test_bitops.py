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

import six

import sys
import operator

import txredisapi as redis
from twisted.internet import defer
from twisted.trial import unittest
from twisted.python import failure

from .mixins import Redis26CheckMixin, REDIS_HOST, REDIS_PORT


class TestBitOps(unittest.TestCase, Redis26CheckMixin):
    _KEYS = ['_bitops_test_key1', '_bitops_test_key2',
             '_bitops_test_key3']

    @defer.inlineCallbacks
    def setUp(self):
        self.db = yield redis.Connection(REDIS_HOST, REDIS_PORT,
                                         reconnect=False,
                                         charset=None)
        self.db1 = None
        self.redis_2_6 = yield self.is_redis_2_6()
        yield self.db.delete(*self._KEYS)
        yield self.db.script_flush()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.db.delete(*self._KEYS)
        yield self.db.disconnect()

    @defer.inlineCallbacks
    def test_getbit(self):
        key = self._KEYS[0]
        yield self.db.set(key, six.b('\xaa'))
        l = [1, 0, 1, 0, 1, 0, 1, 0]
        for x in range(8):
            r = yield self.db.getbit(key, x)
            self.assertEqual(r, l[x])

    @defer.inlineCallbacks
    def test_setbit(self):
        key = self._KEYS[0]
        r = yield self.db.setbit(key, 7, 1)
        self.assertEqual(r, 0)
        r = yield self.db.setbit(key, 7, 0)
        self.assertEqual(r, 1)
        r = yield self.db.setbit(key, 7, True)
        self.assertEqual(r, 0)
        r = yield self.db.setbit(key, 7, False)
        self.assertEqual(r, 1)

    @defer.inlineCallbacks
    def test_bitcount(self):
        self._skipCheck()
        key = self._KEYS[0]
        yield self.db.set(key, "foobar")
        r = yield self.db.bitcount(key)
        self.assertEqual(r, 26)
        r = yield self.db.bitcount(key, 0, 0)
        self.assertEqual(r, 4)
        r = yield self.db.bitcount(key, 1, 1)
        self.assertEqual(r, 6)
        # Ensure that the error is raised
        d = defer.maybeDeferred(self.db.bitcount, key, start=1)
        self.assertFailure(d, redis.RedisError)
        d1 = defer.maybeDeferred(self.db.bitcount, key, end=1)
        self.assertFailure(d1, redis.RedisError)

    def test_bitop_not(self):
        return self._test_bitop([operator.__not__, operator.not_,
                                 'not', 'NOT', 'NoT'],
                                six.b('\x0f\x0f\x0f\x0f'),
                                None,
                                six.b('\xf0\xf0\xf0\xf0'))

    def test_bitop_or(self):
        return self._test_bitop([operator.__or__, operator.or_,
                                 'or', 'OR', 'oR'],
                                six.b('\x0f\x0f\x0f\x0f'),
                                six.b('\xf0\xf0\xf0\xf0'),
                                six.b('\xff\xff\xff\xff'))

    def test_bitop_and(self):
        return self._test_bitop([operator.__and__, operator.and_,
                                 'and', 'AND', 'AnD'],
                                six.b('\x0f\x0f\x0f\x0f'),
                                six.b('\xf0\xf0\xf0\xf0'),
                                six.b('\x00\x00\x00\x00'))

    def test_bitop_xor(self):
        return self._test_bitop([operator.__xor__, operator.xor,
                                 'xor', 'XOR', 'XoR'],
                                six.b('\x9c\x9c\x9c\x9c'),
                                six.b('\x6c\x6c\x6c\x6c'),
                                six.b('\xf0\xf0\xf0\xf0'))

    def test_bitop_invalid(self):
        self.assertFailure(self.db.bitop('test', 'test', 'test'),
                           redis.InvalidData)

    @defer.inlineCallbacks
    def _test_bitop(self, op_list, value1, value2, expected):
        self._skipCheck()
        src_key = self._KEYS[0]
        src_key1 = self._KEYS[1]
        dest_key = self._KEYS[2]
        is_unary = value2 is None
        yield self.db.set(src_key, value1)
        if not is_unary:
            yield self.db.set(src_key1, value2)
            t = (src_key, src_key1)
        else:
            t = (src_key, )
        for op in op_list:
            yield self.db.bitop(op, dest_key, *t)
            r = yield self.db.get(dest_key)
            self.assertEqual(r, expected)
            # Test out failure cases
            # Specify only dest and no src key(s)
            cases = [self.db.bitop(op, dest_key)]
            if is_unary:
                # Try calling unary operator with > 1 operands
                cases.append(self.db.bitop(op, dest_key, src_key, src_key1))
            for case in cases:
                try:
                    r = yield case
                except redis.RedisError:
                    pass
                except:
                    tb = failure.Failure().getTraceback()
                    raise self.failureException('%s raised instead of %s:\n %s'
                                                % (sys.exc_info()[0],
                                                   'txredisapi.RedisError',
                                                   tb))
                else:
                    raise self.failureException('%s not raised (%r returned)'
                                                % ('txredisapi.RedisError',
                                                    r))
