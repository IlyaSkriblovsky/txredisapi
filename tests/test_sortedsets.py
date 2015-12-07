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
from twisted.python.failure import Failure

import txredisapi as redis

from tests.mixins import REDIS_HOST, REDIS_PORT


class SortedSetsTests(unittest.TestCase):
    '''
    Tests for sorted sets
    '''
    _KEYS = ['txredisapi:testssets1', 'txredisapi:testssets2',
             'txredisapi:testssets3', 'txredisapi:testssets4']

    _NUMBERS = ["zero", "one", "two", "three",
                "four", "five", "six", "seven",
                "eight", "nine"]

    @defer.inlineCallbacks
    def test_zaddrem(self):
        key = self._getKey()
        t = self.assertEqual
        r = yield self.db.zadd(key, 1, "one")
        t(r, 1)
        r = yield self.db.zadd(key, 2, "two")
        t(r, 1)
        # Try adding multiple items
        r = yield self.db.zadd(key, 3, "three", 4, "four", 5, "five")
        t(r, 3)
        r = yield self.db.zcount(key, '-inf', '+inf')
        t(r, 5)
        # Try deleting one item
        r = yield self.db.zrem(key, "one")
        # Try deleting some items
        r = yield self.db.zrem(key, "two", "three")
        # Test if calling zadd with odd number of arguments errors out
        yield self.db.zadd(key, 1, "one", 2).addBoth(
            self._check_invaliddata_error, shouldError=True)
        # Now try doing it the right way
        yield self.db.zadd(key, 1, "one", 2, "two").addBoth(
            self._check_invaliddata_error)

    @defer.inlineCallbacks
    def test_zcard_zcount(self):
        key = self._getKey()
        t = self.assertEqual
        yield self._make_sorted_set(key)
        r = yield self.db.zcard(key)   # Check ZCARD
        t(r, 10)
        r = yield self.db.zcount(key)  # ZCOUNT with default args
        t(r, 10)
        r = yield self.db.zcount(key, 1, 5)  # ZCOUNT with args
        t(r, 5)
        r = yield self.db.zcount(key, '(1', 5)  # Exclude arg1
        t(r, 4)
        r = yield self.db.zcount(key, '(1', '(3')  # Exclue arg1 & arg2
        t(r, 1)

    @defer.inlineCallbacks
    def test_zincrby(self):
        key = self._getKey()
        t = self.assertEqual
        yield self._make_sorted_set(key, 1, 3)
        r = yield self.db.zincrby(key, 2, "one")
        t(r, 3)
        r = yield self.db.zrange(key, withscores=True)
        t(r, [('two', 2), ('one', 3)])
        # Also test zincr
        r = yield self.db.zincr(key, "one")
        t(r, 4)
        r = yield self.db.zrange(key, withscores=True)
        t(r, [('two', 2), ('one', 4)])
        # And zdecr
        r = yield self.db.zdecr(key, "one")
        t(r, 3)
        r = yield self.db.zrange(key, withscores=True)
        t(r, [('two', 2), ('one', 3)])

    def test_zrange(self):
        return self._test_zrange(False)

    def test_zrevrange(self):
        return self._test_zrange(True)

    def test_zrank(self):
        return self._test_zrank(False)

    def test_zrevrank(self):
        return self._test_zrank(True)

    @defer.inlineCallbacks
    def test_zscore(self):
        key = self._getKey()
        r, l = yield self._make_sorted_set(key)
        for k, s in l:
            r = yield self.db.zscore(key, k)
            self.assertEqual(r, s)
        r = yield self.db.zscore(key, 'none')
        self.assertTrue(r is None)
        r = yield self.db.zscore('none', 'one')
        self.assertTrue(r is None)

    @defer.inlineCallbacks
    def test_zremrangebyrank(self):
        key = self._getKey()
        t = self.assertEqual
        r, l = yield self._make_sorted_set(key)
        r = yield self.db.zremrangebyrank(key)
        t(r, len(l))
        r = yield self.db.zrange(key)
        t(r, [])  # Check default args
        yield self._make_sorted_set(key, begin=1, end=4)
        r = yield self.db.zremrangebyrank(key, 0, 1)
        t(r, 2)
        r = yield self.db.zrange(key, withscores=True)
        t(r, [('three', 3)])

    @defer.inlineCallbacks
    def test_zremrangebyscore(self):
        key = self._getKey()
        t = self.assertEqual
        r, l = yield self._make_sorted_set(key, end=4)
        r = yield self.db.zremrangebyscore(key)
        t(r, len(l))
        r = yield self.db.zrange(key)
        t(r, [])  # Check default args
        yield self._make_sorted_set(key, begin=1, end=4)
        r = yield self.db.zremrangebyscore(key, '-inf', '(2')
        t(r, 1)
        r = yield self.db.zrange(key, withscores=True)
        t(r, [('two', 2), ('three', 3)])

    def test_zrangebyscore(self):
        return self._test_zrangebyscore(False)

    def test_zrevrangebyscore(self):
        return self._test_zrangebyscore(True)

    def test_zinterstore(self):
        agg_map = {
            'min': (('min', min), {
                    -1: [('three', -3)],
                    0: [('three', 0)],
                    1: [('three', 3)],
                    2: [('three', 3)],
                    }),
            'max': (('max', max), {
                    -1: [('three', 3)],
                    0: [('three', 3)],
                    1: [('three', 3)],
                    2: [('three', 6)],
                    }),
            'sum': (('sum', sum),  {
                    -1: [('three', 0)],
                    0: [('three', 3)],
                    1: [('three', 6)],
                    2: [('three', 9)],
                    })
        }
        return self._test_zunion_inter_store(agg_map)

    def test_zunionstore(self):
        agg_map = {
            'min': (('min', min), {
                -1: [('five', -5), ('four', -4),
                     ('three', -3), ('one', 1),
                     ('two', 2)],
                0: [('five', 0), ('four', 0), ('three', 0),
                    ('one', 1), ('two', 2)],
                1: [('one', 1), ('two', 2), ('three', 3),
                    ('four', 4), ('five', 5)],
                2: [('one', 1), ('two', 2), ('three', 3),
                    ('four', 8), ('five', 10)]
            }),
            'max': (('max', max), {
                -1: [('five', -5), ('four', -4),
                     ('one', 1), ('two', 2),
                     ('three', 3)],
                0: [('five', 0), ('four', 0), ('one', 1),
                    ('two', 2), ('three', 3)],
                1: [('one', 1), ('two', 2), ('three', 3),
                    ('four', 4), ('five', 5)],
                2: [('one', 1), ('two', 2), ('three', 6),
                    ('four', 8), ('five', 10)]
            }),
            'sum': (('sum', sum),  {
                -1: [('five', -5), ('four', -4),
                     ('three', 0), ('one', 1), ('two', 2)],
                0: [('five', 0), ('four', 0), ('one', 1),
                    ('two', 2), ('three', 3)],
                1: [('one', 1), ('two', 2), ('four', 4),
                    ('five', 5), ('three', 6)],
                2: [('one', 1), ('two', 2), ('four', 8),
                    ('three', 9), ('five', 10)]
            })
        }
        return self._test_zunion_inter_store(agg_map, True)

    @defer.inlineCallbacks
    def _test_zunion_inter_store(self, agg_function_map, union=False):
        if union:
            cmd = self.db.zunionstore
        else:
            cmd = self.db.zinterstore
        key = self._getKey()
        t = self.assertEqual
        key1 = self._getKey(1)
        destKey = self._getKey(2)
        r, l = yield self._make_sorted_set(key, begin=1, end=4)
        r1, l1 = yield self._make_sorted_set(key1, begin=3, end=6)
        for agg_fn_name in agg_function_map:
            for agg_fn in agg_function_map[agg_fn_name][0]:
                for key1_weight in range(-1, 3):
                    if key1_weight == 1:
                        keys = [key, key1]
                    else:
                        keys = {key: 1, key1: key1_weight}
                    r = yield cmd(destKey, keys, aggregate=agg_fn)
                    if union:
                        t(r, len(set(l + l1)))
                    else:
                        t(r, len(set(l) & set(l1)))
                    r = yield self.db.zrange(destKey, withscores=True)
                    t(r, agg_function_map[agg_fn_name][1][key1_weight])
                    yield self.db.delete(destKey)
        # Finally, test for invalid aggregate functions
        yield self.db.delete(key, key1)
        yield self._make_sorted_set(key, begin=1, end=4)
        yield self._make_sorted_set(key1, begin=3, end=6)
        yield cmd(destKey, [key, key1], aggregate='SIN').addBoth(
            self._check_invaliddata_error, shouldError=True)
        yield cmd(destKey, [key, key1], aggregate=lambda a, b: a + b).addBoth(
            self._check_invaliddata_error, shouldError=True)
        yield self.db.delete(destKey)

    @defer.inlineCallbacks
    def _test_zrangebyscore(self, reverse):
        key = self._getKey()
        t = self.assertEqual
        if reverse:
            command = self.db.zrevrangebyscore
        else:
            command = self.db.zrangebyscore
        for ws in [True, False]:
            r, l = yield self._make_sorted_set(key, begin=1, end=4)
            if reverse:
                l.reverse()
            r = yield command(key, withscores=ws)
            if ws:
                t(r, l)
            else:
                t(r, [x[0] for x in l])
            r = yield command(key, withscores=ws, offset=1, count=1)
            if ws:
                t(r, [('two', 2)])
            else:
                t(r, ['two'])
            yield self.db.delete(key)
        # Test for invalid offset and count
        yield self._make_sorted_set(key, begin=1, end=4)
        yield command(key, offset=1).addBoth(
            self._check_invaliddata_error, shouldError=True)
        yield command(key, count=1).addBoth(
            self._check_invaliddata_error, shouldError=True)

    @defer.inlineCallbacks
    def _test_zrank(self, reverse):
        key = self._getKey()
        r, l = yield self._make_sorted_set(key)
        if reverse:
            command = self.db.zrevrank
            l.reverse()
        else:
            command = self.db.zrank
        for k, s in l:
            r = yield command(key, k)
            self.assertEqual(l[r][0], k)
        r = yield command(key, 'none')  # non-existant member
        self.assertTrue(r is None)
        r = yield command('none', 'one')
        self.assertTrue(r is None)

    @defer.inlineCallbacks
    def _test_zrange(self, reverse):
        key = self._getKey()
        t = self.assertEqual
        r, l = yield self._make_sorted_set(key)
        if reverse:
            command = self.db.zrevrange
            l.reverse()
        else:
            command = self.db.zrange
        r = yield command(key)
        t(r, [x[0] for x in l])
        r = yield command(key, withscores=True)
        # Ensure that WITHSCORES returns tuples
        t(r, l)
        # Test with args
        r = yield command(key, start='5', end='8', withscores=True)
        t(r, l[5:9])
        # Test to ensure empty results return empty lists
        r = yield command(key, start=-20, end=-40, withscores=True)
        t(r, [])

    def _getKey(self, n=0):
        return self._KEYS[n]

    def _to_words(self, n):
        l = []
        while True:
            n, r = divmod(n, 10)
            l.append(self._NUMBERS[r])
            if n == 0:
                break
        return ' '.join(l)

    def _sorted_set_check(self, r, l):
        self.assertEqual(r, len(l))
        return r, l

    def _make_sorted_set(self, key, begin=0, end=10):
        l = []
        for x in range(begin, end):
            l.extend((x, self._to_words(x)))
        return self.db.zadd(key, *l).addCallback(
            self._sorted_set_check, list(zip(l[1::2], l[::2])))

    @defer.inlineCallbacks
    def setUp(self):
        self.db = yield redis.Connection(REDIS_HOST, REDIS_PORT,
                                         reconnect=False)

    def tearDown(self):
        return defer.gatherResults(
            [self.db.delete(x) for x in self._KEYS]).addCallback(
                lambda ign: self.db.disconnect())

    def _check_invaliddata_error(self, response, shouldError=False):
        if shouldError:
            self.assertIsInstance(response, Failure)
            self.assertIsInstance(response.value, redis.InvalidData)
        else:
            self.assertNotIsInstance(response, Failure)
