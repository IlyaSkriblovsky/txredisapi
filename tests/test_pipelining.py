# coding: utf-8
# Copyright 2013 Matt Pizzimenti
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

import six

from twisted.trial import unittest
from twisted.internet import defer
from twisted.python import log

import txredisapi

from tests.mixins import REDIS_HOST, REDIS_PORT

# log.startLogging(sys.stdout)


class InspectableTransport(object):

    def __init__(self, transport):
        self.original_transport = transport
        self.write_history = []

    def __getattr__(self, method):

        if method == "write":
            def write(data, *args, **kwargs):
                self.write_history.append(data)
                return self.original_transport.write(data, *args, **kwargs)
            return write
        return getattr(self.original_transport, method)


class TestRedisConnections(unittest.TestCase):

    @defer.inlineCallbacks
    def _assert_simple_sets_on_pipeline(self, db):

        pipeline = yield db.pipeline()
        self.assertTrue(pipeline.pipelining)

        # Hook into the transport so we can inspect what is happening
        # at the protocol level.
        pipeline.transport = InspectableTransport(pipeline.transport)

        pipeline.set("txredisapi:test_pipeline", "foo")
        pipeline.set("txredisapi:test_pipeline", "bar")
        pipeline.set("txredisapi:test_pipeline2", "zip")

        yield pipeline.execute_pipeline()
        self.assertFalse(pipeline.pipelining)

        result = yield db.get("txredisapi:test_pipeline")
        self.assertEqual(result, "bar")

        result = yield db.get("txredisapi:test_pipeline2")
        self.assertEqual(result, "zip")

        # Make sure that all SET commands were sent in a single pipelined write.
        write_history = pipeline.transport.write_history
        lines_in_first_write = write_history[0].split(six.b("\n"))
        sets_in_first_write = sum(1 for w in lines_in_first_write if six.b("SET") in w)
        self.assertEqual(sets_in_first_write, 3)

    @defer.inlineCallbacks
    def _wait_for_lazy_connection(self, db):

        # For lazy connections, wait for the internal deferred to indicate
        # that the connection is established.
        yield db._connected

    @defer.inlineCallbacks
    def test_Connection(self):

        db = yield txredisapi.Connection(REDIS_HOST, REDIS_PORT,
                                         reconnect=False)
        yield self._assert_simple_sets_on_pipeline(db=db)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ConnectionDB1(self):

        db = yield txredisapi.Connection(REDIS_HOST, REDIS_PORT, dbid=1,
                                         reconnect=False)
        yield self._assert_simple_sets_on_pipeline(db=db)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ConnectionPool(self):

        db = yield txredisapi.ConnectionPool(REDIS_HOST, REDIS_PORT, poolsize=2,
                                             reconnect=False)
        yield self._assert_simple_sets_on_pipeline(db=db)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ConnectionPool_managed_correctly(self):

        db = yield txredisapi.ConnectionPool(REDIS_HOST, REDIS_PORT, poolsize=1,
                                             reconnect=False)

        yield db.set('key1', 'value1')

        pipeline = yield db.pipeline()
        pipeline.get('key1')

        # We will yield after we finish the pipeline so we won't block here
        d = db.set('key2', 'value2')

        results = yield pipeline.execute_pipeline()

        # If the pipeline is managed correctly, there should only be one
        # response here
        self.assertEqual(len(results), 1)

        yield d
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyConnection(self):

        db = txredisapi.lazyConnection(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield self._wait_for_lazy_connection(db)
        yield self._assert_simple_sets_on_pipeline(db=db)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyConnectionPool(self):

        db = txredisapi.lazyConnectionPool(REDIS_HOST, REDIS_PORT, reconnect=False)
        yield self._wait_for_lazy_connection(db)
        yield self._assert_simple_sets_on_pipeline(db=db)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ShardedConnection(self):

        hosts = ["%s:%s" % (REDIS_HOST, REDIS_PORT)]
        db = yield txredisapi.ShardedConnection(hosts, reconnect=False)
        try:
            yield db.pipeline()
            raise self.failureException("Expected sharding to disallow pipelining")
        except NotImplementedError as e:
            self.assertTrue("not supported" in str(e).lower())
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ShardedConnectionPool(self):

        hosts = ["%s:%s" % (REDIS_HOST, REDIS_PORT)]
        db = yield txredisapi.ShardedConnectionPool(hosts, reconnect=False)
        try:
            yield db.pipeline()
            raise self.failureException("Expected sharding to disallow pipelining")
        except NotImplementedError as e:
            self.assertTrue("not supported" in str(e).lower())
        yield db.disconnect()
