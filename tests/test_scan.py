#!/usr/bin/env python
# coding: utf-8
#
# Copyright 2014 Ilia Glazkov
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


from txredisapi import Connection

from twisted.internet.defer import inlineCallbacks
from twisted.trial import unittest

from .mixins import RedisVersionCheckMixin, REDIS_HOST, REDIS_PORT


class TestScan(unittest.TestCase, RedisVersionCheckMixin):
    KEYS = ['_scan_test_' + str(v).zfill(4) for v in range(100)]
    SKEY = ['_scan_test_set']
    SUFFIX = '12'
    PATTERN = '_scan_test_*' + SUFFIX

    @inlineCallbacks
    def setUp(self):
        self.FILTERED_KEYS = [k for k in self.KEYS if k.endswith(self.SUFFIX)]
        self.db = yield Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        self.redis_2_8_0 = yield self.checkVersion(2, 8, 0)
        yield self.db.delete(*self.KEYS)
        yield self.db.delete(self.SKEY)

    @inlineCallbacks
    def tearDown(self):
        yield self.db.delete(*self.KEYS)
        yield self.db.delete(self.SKEY)
        yield self.db.disconnect()

    @inlineCallbacks
    def test_scan(self):
        self._skipCheck()
        yield self.db.mset(dict((k, 'value') for k in self.KEYS))

        cursor, result = yield self.db.scan(pattern=self.PATTERN)

        while cursor != 0:
            cursor, keys = yield self.db.scan(cursor, pattern=self.PATTERN)
            result.extend(keys)

        self.assertEqual(set(result), set(self.FILTERED_KEYS))

    @inlineCallbacks
    def test_sscan(self):
        self._skipCheck()
        yield self.db.sadd(self.SKEY, self.KEYS)

        cursor, result = yield self.db.sscan(self.SKEY, pattern=self.PATTERN)

        while cursor != 0:
            cursor, keys = yield self.db.sscan(self.SKEY, cursor,
                                               pattern=self.PATTERN)
            result.extend(keys)

        self.assertEqual(set(result), set(self.FILTERED_KEYS))

    def _skipCheck(self):
        if not self.redis_2_8_0:
            skipMsg = "Redis version < 2.8.0 (found version: %s)"
            raise unittest.SkipTest(skipMsg % self.redis_version)
