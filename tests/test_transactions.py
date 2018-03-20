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

import sys

from twisted.trial import unittest
from twisted.internet import defer
from twisted.python import log

import txredisapi

from tests.mixins import REDIS_HOST, REDIS_PORT

# log.startLogging(sys.stdout)


class TestRedisConnections(unittest.TestCase):
    @defer.inlineCallbacks
    def testRedisConnection(self):
        rapi = yield txredisapi.Connection(REDIS_HOST, REDIS_PORT)

        # test set() operation
        transaction = yield rapi.multi("txredisapi:test_transaction")
        self.assertTrue(transaction.inTransaction)
        for key, value in (("txredisapi:test_transaction", "foo"),
                           ("txredisapi:test_transaction", "bar")):
            yield transaction.set(key, value)
        yield transaction.commit()
        self.assertFalse(transaction.inTransaction)
        result = yield rapi.get("txredisapi:test_transaction")
        self.assertEqual(result, "bar")

        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testRedisWithOnlyWatchUnwatch(self):
        rapi = yield txredisapi.Connection(REDIS_HOST, REDIS_PORT)

        k = "txredisapi:testRedisWithOnlyWatchAndUnwatch"
        tx = yield rapi.watch(k)
        self.assertTrue(tx.inTransaction)
        yield tx.set(k, "bar")
        v = yield tx.get(k)
        self.assertEqual("bar", v)
        yield tx.unwatch()
        self.assertFalse(tx.inTransaction)

        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testRedisWithWatchAndMulti(self):
        rapi = yield txredisapi.Connection(REDIS_HOST, REDIS_PORT)

        tx = yield rapi.watch("txredisapi:testRedisWithWatchAndMulti")
        yield tx.multi()
        yield tx.unwatch()
        self.assertTrue(tx.inTransaction)
        yield tx.commit()
        self.assertFalse(tx.inTransaction)

        yield rapi.disconnect()

    # some sort of probabilistic test
    @defer.inlineCallbacks
    def testWatchAndPools_1(self):
        rapi = yield txredisapi.ConnectionPool(REDIS_HOST, REDIS_PORT,
                                               poolsize=2, reconnect=False)
        tx1 = yield rapi.watch("foobar")
        tx2 = yield tx1.watch("foobaz")
        self.assertTrue(id(tx1) == id(tx2))
        yield rapi.disconnect()

    # some sort of probabilistic test
    @defer.inlineCallbacks
    def testWatchAndPools_2(self):
        rapi = yield txredisapi.ConnectionPool(REDIS_HOST, REDIS_PORT,
                                               poolsize=2, reconnect=False)
        tx1 = yield rapi.watch("foobar")
        tx2 = yield rapi.watch("foobaz")
        self.assertTrue(id(tx1) != id(tx2))
        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testWatchEdgeCase_1(self):
        rapi = yield txredisapi.Connection(REDIS_HOST, REDIS_PORT)

        tx = yield rapi.multi("foobar")
        yield tx.unwatch()
        self.assertTrue(tx.inTransaction)
        yield tx.discard()
        self.assertFalse(tx.inTransaction)

        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testWatchEdgeCase_2(self):
        rapi = yield txredisapi.Connection(REDIS_HOST, REDIS_PORT)

        tx = yield rapi.multi()
        try:
            yield tx.watch("foobar")
        except txredisapi.ResponseError:
            pass
        yield tx.unwatch()
        self.assertTrue(tx.inTransaction)
        yield tx.discard()
        self.assertFalse(tx.inTransaction)
        yield rapi.disconnect()

    @defer.inlineCallbacks
    def testWatchEdgeCase_3(self):
        rapi = yield txredisapi.Connection(REDIS_HOST, REDIS_PORT)

        tx = yield rapi.watch("foobar")
        tx = yield tx.multi("foobaz")
        yield tx.unwatch()
        self.assertTrue(tx.inTransaction)
        yield tx.discard()
        self.assertFalse(tx.inTransaction)

        yield rapi.disconnect()
