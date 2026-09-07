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

from twisted.internet import defer, reactor, task
from twisted.internet.protocol import Factory
from twisted.trial import unittest

import txredisapi as redis

from tests.test_sentinel import FakeRedisFactory


class TrackingRedisFactory(FakeRedisFactory):
    """Fake server that keeps every protocol it has built."""

    def __init__(self):
        self.protocols = []

    def buildProtocol(self, addr):
        protocol = FakeRedisFactory.buildProtocol(self, addr)
        self.protocols.append(protocol)
        return protocol


class TestReconnect(unittest.TestCase):
    timeout = 30

    def setUp(self):
        self.server = TrackingRedisFactory()
        self.listener = reactor.listenTCP(0, self.server,
                                          interface="127.0.0.1")
        self.port = self.listener.getHost().port
        self.addCleanup(self.listener.stopListening)

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
    def test_disconnect_cancels_reconnection(self):
        db = yield redis.Connection("127.0.0.1", self.port)
        yield db.role()

        self.server.protocols[0].transport.loseConnection()
        yield db.disconnect()

        # a scheduled reconnection attempt must not survive disconnect()
        yield task.deferLater(reactor, 0.3, lambda: None)
        self.assertEqual(len(self.server.protocols), 1)
