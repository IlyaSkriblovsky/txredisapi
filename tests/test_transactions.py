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

import txredisapi
from twisted.trial import unittest
from twisted.internet import defer, reactor
from twisted.python import log                                                  
import sys

log.startLogging(sys.stdout)

redis_host="localhost"
redis_port=6379

class TestRedisConnections(unittest.TestCase):
    @defer.inlineCallbacks
    def testRedisConnection(self):
        rapi = yield txredisapi.Connection(redis_host, redis_port)
        
        # test set() operation
        transaction = yield rapi.multi()
        print "oi" 
        for key, value in (("txredisapi:test_transaction", "foo"), ("txredisapi:test_transaction", "bar")):
            yield transaction.set(key, value)
        yield transaction.commit()
        result = yield rapi.get("txredisapi:test_transaction")
        self.assertEqual(result, "bar")

        yield rapi.disconnect()
