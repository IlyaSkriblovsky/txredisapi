# coding: utf-8
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

from twisted.internet import defer, error, reactor, task
from twisted.internet.endpoints import HostnameEndpoint
from twisted.internet.interfaces import IStreamClientEndpoint
from twisted.internet.protocol import Factory
from twisted.trial import unittest
from zope.interface import implementer

import txredisapi as redis

from tests.test_sentinel import TrackingRedisFactory


@implementer(IStreamClientEndpoint)
class FailingEndpoint(object):
    """
    Endpoint that fails synchronously, the way UNIXClientEndpoint does when
    the socket does not exist.
    """

    def __init__(self):
        self.attempts = 0

    def connect(self, protocolFactory):
        self.attempts += 1
        return defer.fail(error.ConnectionRefusedError())


@implementer(IStreamClientEndpoint)
class GatedEndpoint(object):
    """
    Endpoint that lets the first connection through and refuses the ones
    after it, but not before the given gate fires.
    """

    def __init__(self, endpoint, gate):
        self.endpoint = endpoint
        self.gate = gate
        self.attempts = 0

    def connect(self, protocolFactory):
        self.attempts += 1
        if self.attempts == 1:
            return self.endpoint.connect(protocolFactory)

        refused = defer.Deferred()
        self.gate.addCallback(
            lambda _: refused.errback(error.ConnectionRefusedError()))
        return refused


class TestReconnect(unittest.TestCase):
    timeout = 30

    def setUp(self):
        self.server = TrackingRedisFactory()
        self.listener = reactor.listenTCP(0, self.server,
                                          interface="127.0.0.1")
        self.port = self.listener.getHost().port
        self.addCleanup(self.listener.stopListening)

    def endpoint(self):
        return HostnameEndpoint(reactor, "127.0.0.1", self.port)

    @defer.inlineCallbacks
    def waitFor(self, predicate, message):
        deadline = reactor.seconds() + 15
        while not predicate():
            if reactor.seconds() > deadline:
                self.fail(message)
            yield task.deferLater(reactor, 0.05, lambda: None)

    @defer.inlineCallbacks
    def closedPort(self):
        listener = reactor.listenTCP(0, Factory(), interface="127.0.0.1")
        port = listener.getHost().port
        yield listener.stopListening()
        return port

    @defer.inlineCallbacks
    def test_reconnects(self):
        db = yield redis.Connection("127.0.0.1", self.port)
        self.addCleanup(db.disconnect)
        yield db.role()

        self.server.protocols[0].transport.loseConnection()

        yield self.waitFor(lambda: len(self.server.protocols) > 1,
                           "connection has not been reestablished")
        role = yield db.role()
        self.assertEqual(role[0], "master")

    @defer.inlineCallbacks
    def test_does_not_reconnect_when_disabled(self):
        # short enough for the wait below to cover several attempts
        self.patch(redis.RedisFactory, "initialDelay", 0.05)

        db = yield redis.Connection("127.0.0.1", self.port, reconnect=False)
        self.addCleanup(db.disconnect)
        yield db.role()

        self.server.protocols[0].transport.loseConnection()
        yield self.waitFor(lambda: db._factory.size == 0,
                           "connection has not been dropped")

        # give a reconnect a chance to happen, it should not
        yield task.deferLater(reactor, 0.3, lambda: None)
        self.assertEqual(len(self.server.protocols), 1)

        yield self.assertFailure(db.role(), redis.ConnectionError)

    @defer.inlineCallbacks
    def test_failed_connection_is_reported(self):
        port = yield self.closedPort()
        yield self.assertFailure(
            redis.Connection("127.0.0.1", port, reconnect=False), ValueError)

    @defer.inlineCallbacks
    def test_first_failure_is_final(self):
        # long enough for a second attempt to be noticed as a hang
        self.patch(redis.RedisFactory, "initialDelay", 30)

        endpoint = FailingEndpoint()
        factory = redis.RedisFactory(None, dbid=None, poolsize=1)
        factory.reconnect = False
        deferred = factory.deferred
        factory.startConnecting(endpoint)

        yield self.assertFailure(deferred, ValueError)
        self.assertEqual(endpoint.attempts, 1)

    @defer.inlineCallbacks
    def test_failed_pool_startup_closes_connections(self):
        gate = defer.Deferred()
        endpoint = GatedEndpoint(self.endpoint(), gate)

        factory = redis.RedisFactory(None, dbid=None, poolsize=2)
        factory.reconnect = False
        deferred = factory.deferred
        factory.startConnecting(endpoint)

        yield self.waitFor(lambda: factory.size == 1,
                           "the first connection has not been established")
        gate.callback(None)

        yield self.assertFailure(deferred, ValueError)

        # the caller got an error instead of the handler, so it can't close
        # the connection that did come up - we have to
        yield self.waitFor(lambda: not self.server.protocols[0].connected,
                           "the established connection has not been closed")

    @defer.inlineCallbacks
    def test_failed_lazy_pool_keeps_working_connections(self):
        gate = defer.Deferred()
        endpoint = GatedEndpoint(self.endpoint(), gate)

        factory = redis.RedisFactory(None, dbid=None, poolsize=2, isLazy=True)
        factory.reconnect = False
        # nobody consumes the Deferred of a lazy factory
        factory.deferred.addErrback(lambda failure: None)
        factory.startConnecting(endpoint)

        yield self.waitFor(lambda: factory.size == 1,
                           "the first connection has not been established")
        gate.callback(None)
        yield task.deferLater(reactor, 0.3, lambda: None)

        # the handler is in the caller's hands already, so the connection that
        # did come up stays usable
        self.assertEqual(factory.size, 1)
        role = yield factory.handler.role()
        self.assertEqual(role[0], "master")

        yield factory.handler.disconnect()

    @defer.inlineCallbacks
    def test_disconnect_cancels_reconnection(self):
        # long enough for the reconnection to still be scheduled below
        self.patch(redis.RedisFactory, "initialDelay", 0.5)

        db = yield redis.Connection("127.0.0.1", self.port)
        yield db.role()

        self.server.protocols[0].transport.loseConnection()
        yield self.waitFor(lambda: db._factory.size == 0,
                           "connection has not been dropped")

        # a scheduled reconnection attempt must not survive disconnect()
        yield db.disconnect()
        yield task.deferLater(reactor, 1, lambda: None)
        self.assertEqual(len(self.server.protocols), 1)

    @defer.inlineCallbacks
    def test_stop_trying_from_connection_made(self):
        # the pattern examples/subscriber.py shows: give up on the connection
        # from inside connectionMade, while the endpoint is still finishing
        # it. Neither hook chains to the base protocol, the way protocols
        # written for a SubscriberFactory usually don't
        class StoppingProtocol(redis.RedisProtocol):
            def connectionMade(self):
                self.factory.stopTrying()
                self.transport.loseConnection()

            def connectionLost(self, reason):
                pass

        factory = redis.RedisFactory(None, dbid=None, poolsize=1)
        factory.protocol = StoppingProtocol
        factory.startConnecting(self.endpoint())

        yield self.waitFor(lambda: self.server.protocols and
                           not self.server.protocols[0].connected,
                           "the connection has not been closed")

        yield task.deferLater(reactor, 0.3, lambda: None)
        self.assertEqual(len(self.server.protocols), 1)

    def test_dead_connection_is_not_pooled(self):
        factory = redis.RedisFactory(None, dbid=None, poolsize=1)
        conn = factory.buildProtocol(None)
        # died during the reactor turn addConnection is delayed by
        conn.connected = 0

        factory.addConnection(conn)
        self.assertEqual(factory.size, 0)

    def test_retry_delay_does_not_overflow(self):
        factory = redis.RedisFactory(None, dbid=None, poolsize=1)
        # a long outage keeps increasing the attempt number
        self.assertLess(factory.retryDelay(10 ** 6), factory.maxDelay * 2)
