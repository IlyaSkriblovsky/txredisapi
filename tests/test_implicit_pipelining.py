# coding: utf-8
# Copyright 2015 Ilya Skriblovsky
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

cnt_LineReceiver = 0
cnt_HiredisProtocol = 0


class _CallCounter(object):
    def __init__(self, original):
        self.call_count = 0
        self.original = original

    def get_callee(self):
        def callee(this, *args, **kwargs):
            self.call_count += 1
            return self.original(this, *args, **kwargs)
        return callee


class TestImplicitPipelining(unittest.TestCase):
    KEY = 'txredisapi:key'
    VALUE = 'txredisapi:value'

    @defer.inlineCallbacks
    def testImplicitPipelining(self):
        """
        Calling query method several times in a row without waiting should
        do implicit pipelining, so all requests are send immediately and
        all replies are received in one chunk with high probability
        """
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)

        cnt_LineReceiver = _CallCounter(redis.LineReceiver.dataReceived)
        self.patch(redis.LineReceiver, 'dataReceived',
                   cnt_LineReceiver.get_callee())
        cnt_HiredisProtocol = _CallCounter(redis.HiredisProtocol.dataReceived)
        self.patch(redis.HiredisProtocol, 'dataReceived',
                   cnt_HiredisProtocol.get_callee())

        for i in range(5):
            db.set(self.KEY, self.VALUE)

        yield db.get(self.KEY)

        total_data_chunks = cnt_LineReceiver.call_count + \
            cnt_HiredisProtocol.call_count
        self.assertEqual(total_data_chunks, 1)

        yield db.disconnect()
