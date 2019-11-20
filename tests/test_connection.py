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
from twisted.internet import base
from twisted.internet import defer
from twisted.internet.task import Clock
from twisted.test.iosim import connectedServerAndClient
from twisted.trial import unittest

import txredisapi as redis

from tests.mixins import REDIS_HOST, REDIS_PORT
from tests.test_sentinel import FakeRedisProtocol, FakeRedisFactory

base.DelayedCall.debug = False


class TestConnectionMethods(unittest.TestCase):
    @defer.inlineCallbacks
    def test_Connection(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)
        self.assertEqual(isinstance(db, redis.ConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ConnectionDB1(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, dbid=1,
                                    reconnect=False)
        self.assertEqual(isinstance(db, redis.ConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ConnectionPool(self):
        db = yield redis.ConnectionPool(REDIS_HOST, REDIS_PORT, poolsize=2,
                                        reconnect=False)
        self.assertEqual(isinstance(db, redis.ConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyConnection(self):
        db = redis.lazyConnection(REDIS_HOST, REDIS_PORT, reconnect=False)
        self.assertEqual(isinstance(db._connected, defer.Deferred), True)
        db = yield db._connected
        self.assertEqual(isinstance(db, redis.ConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_lazyConnectionPool(self):
        db = redis.lazyConnectionPool(REDIS_HOST, REDIS_PORT, reconnect=False)
        self.assertEqual(isinstance(db._connected, defer.Deferred), True)
        db = yield db._connected
        self.assertEqual(isinstance(db, redis.ConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ShardedConnection(self):
        hosts = ["%s:%s" % (REDIS_HOST, REDIS_PORT)]
        db = yield redis.ShardedConnection(hosts, reconnect=False)
        self.assertEqual(isinstance(db, redis.ShardedConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_ShardedConnectionPool(self):
        hosts = ["%s:%s" % (REDIS_HOST, REDIS_PORT)]
        db = yield redis.ShardedConnectionPool(hosts, reconnect=False)
        self.assertEqual(isinstance(db, redis.ShardedConnectionHandler), True)
        yield db.disconnect()

    @defer.inlineCallbacks
    def test_dbid(self):
        db1 = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False, dbid=1)
        db2 = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False, dbid=5)
        self.addCleanup(db1.disconnect)
        self.addCleanup(db2.disconnect)

        yield db1.set('x', 42)
        try:
            value = yield db2.get('x')
            self.assertIs(value, None)
        finally:
            yield db1.delete('x')


class SlowServerProtocol(FakeRedisProtocol):
    def __init__(self, reactor, delay, storage):
        FakeRedisProtocol.__init__(self)
        self.callLater = reactor.callLater
        self.delay = delay
        self.storage = storage
        self._delayed_calls = []

    def replyReceived(self, request):
        if isinstance(request, list):
            command = request[0]
            if command == "GET":
                name = request[1]
                if name in self.storage:
                    self.send_reply(self.storage[name])
            elif command == "BLPOP":
                name = request[1]
                timeout = request[2]

                def do_reply():
                    try:
                        value = self.storage[name].pop(0)
                    except (AttributeError, IndexError):
                        value = None
                    self.send_reply(value)
                self.callLater(timeout, do_reply)
            else:
                self.send_error("Command not supported")
        else:
            FakeRedisProtocol.replyReceived(self, request)

    def send_reply(self, reply):
        self._delayed_calls.append(
            self.callLater(self.delay, FakeRedisProtocol.send_reply, self, reply)
        )

    def send_error(self, msg):
        self._delayed_calls.append(
            self.callLater(self.delay, FakeRedisProtocol.send_error, self, msg)
        )

    def connectionLost(self, why):
        for call in self._delayed_calls:
            if call.active():
                call.cancel()


class TestTimeouts(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()

        old = redis.LineReceiver.callLater
        redis.LineReceiver.callLater = self.clock.callLater

        def cleanup():
            redis.LineReceiver.callLater = old
        self.addCleanup(cleanup)


    def _clientAndServer(self, clientTimeout, serverTimeout, initialStorage=None):
        storage = initialStorage or {}
        return connectedServerAndClient(
            lambda: SlowServerProtocol(self.clock, serverTimeout, storage),
            lambda: redis.RedisProtocol(replyTimeout=clientTimeout)
        )

    def test_noTimeout(self):
        client, server, pump = self._clientAndServer(2, 1, {'x': 42})

        result = client.get('x')
        pump.flush()
        self.assertFalse(result.called)
        self.clock.pump([0, 1.1])
        pump.flush()
        self.assertEqual(self.successResultOf(result), 42)

        # ensure that idle connection is not closed by timeout
        self.clock.pump([0, 5])
        result2 = client.get('x')
        pump.flush()
        self.clock.pump([0, 1.1])
        pump.flush()
        self.assertEqual(self.successResultOf(result2), 42)


    def test_timedOut(self):
        client, server, pump = self._clientAndServer(0.5, 1, {'x': 42})

        result = client.get('x')
        pump.flush()
        self.assertFalse(result.called)
        self.clock.pump([0, 0.6])
        pump.flush()
        self.failureResultOf(result, redis.TimeoutError)


    def test_timeoutBreaksConnection(self):
        """
        When the call is timed out connection to Redis is closed and all
        pending queries are interrupted with ConnectionError (because they
        are not timed out yet themselves)
        """
        client, server, pump = self._clientAndServer(0.5, 1, {'x': 42})

        result1 = client.get('x')
        pump.flush()
        self.clock.pump([0, 0.3])
        result2 = client.get('x')
        pump.flush()
        self.clock.pump([0, 0.3])
        pump.flush()
        self.failureResultOf(result1, redis.TimeoutError)
        self.failureResultOf(result2, redis.ConnectionError)


    def test_blockingOps(self):
        """
        replyTimeout should not be applied to blocking commands:
        blpop, brpop, brpoplpush
        """
        client, server, pump = self._clientAndServer(1, 0, {'x': 42})

        result1 = client.blpop('x', timeout=5)
        pump.flush()
        self.clock.pump([0, 4])
        pump.flush()
        self.assertNoResult(result1)
        self.clock.pump([0, 1.1])
        pump.flush()
        self.assertEqual(self.successResultOf(result1), None)



class TestTimeoutsOnRealServer(unittest.TestCase):
    @defer.inlineCallbacks
    def test_blockingOps(self):
        """
        replyTimeout should not be applied to blocking commands:
        blpop, brpop, brpoplpush
        """
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, dbid=1,
                                    reconnect=False, replyTimeout=1)
        self.addCleanup(db.disconnect)

        result1 = yield db.brpop('x', timeout=2)
        self.assertIs(result1, None)
