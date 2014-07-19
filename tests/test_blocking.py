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


class TestBlockingCommands(unittest.TestCase):
    QUEUE_KEY = 'txredisapi:test_queue'
    TEST_KEY = 'txredisapi:test_key'
    QUEUE_VALUE = 'queue_value'

    @defer.inlineCallbacks
    def testBlocking(self):
        db = yield redis.ConnectionPool(REDIS_HOST, REDIS_PORT, poolsize=2,
                                        reconnect=False)
        yield db.delete(self.QUEUE_KEY, self.TEST_KEY)

        # Block first connection.
        d = db.brpop(self.QUEUE_KEY, timeout=3)
        # Use second connection.
        yield db.set(self.TEST_KEY, 'somevalue')
        # Should use second connection again. Will block till end of
        # brpop otherwise.
        yield db.lpush('txredisapi:test_queue', self.QUEUE_VALUE)

        brpop_result = yield d
        self.assertNotEqual(brpop_result, None)

        yield db.delete(self.QUEUE_KEY, self.TEST_KEY)
        yield db.disconnect()
