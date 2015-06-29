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

from twisted.internet import defer, reactor
from twisted.trial import unittest

import txredisapi as redis

from tests.mixins import REDIS_HOST, REDIS_PORT


class TestRedisCancels(unittest.TestCase):
    @defer.inlineCallbacks
    def test_cancel(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT)

        prefix = 'txredisapi:cancel'

        # Set + Get
        key = prefix + '1'
        value = 'first'
        res = yield db.set(key, value)
        self.assertEquals('OK', res)
        val = yield db.get(key)
        self.assertEquals(val, value)

        # Cancel a method
        d = db.time()
        d.addErrback(lambda _: True)
        d.cancel()

        # And Set + Get 
        key = prefix + '2'
        value = 'second'
        res = yield db.set(key, value)
        #self.assertEquals('OK', res)
        val = yield db.get(key)
        self.assertEquals(val, value)

        yield db.disconnect()
