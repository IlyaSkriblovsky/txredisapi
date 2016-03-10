# coding: utf-8
# Copyright 2015 Jeethu Rao
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

import six

import txredisapi as redis

from twisted.trial import unittest
from twisted.internet import reactor, defer, error, protocol
from twisted.internet.protocol import ClientFactory
from twisted.test.proto_helpers import StringTransportWithDisconnection
from twisted.internet import task

class MockFactory(ClientFactory):
    pass


class LineReceiverSubclass(redis.LineReceiver):
    def lineReceived(self, line):
        self._rcvd_line = line

    def rawDataReceived(self, data):
        self._rcvd_data = data


class TestLineReciever(unittest.TestCase):
    S = six.b('TEST')

    def setUp(self):
        self.proto = LineReceiverSubclass()
        self.transport = StringTransportWithDisconnection()
        self.proto.makeConnection(self.transport)
        self.transport.protocol = self.proto
        self.proto.factory = MockFactory()

    def test_excess_line_length(self):
        self.assertTrue(self.transport.connected)
        self.proto.dataReceived(six.b('\x00') * (self.proto.MAX_LENGTH + 1))
        self.assertFalse(self.transport.connected)

    def test_excess_delimited_line(self):
        self.assertTrue(self.transport.connected)
        self.proto.dataReceived(self.S + self.proto.delimiter)
        self.assertEqual(self.proto._rcvd_line, self.S.decode())
        s = (six.b('\x00') * (self.proto.MAX_LENGTH + 1)) + self.proto.delimiter
        self.proto._rcvd_line = None
        self.proto.dataReceived(s)
        self.assertFalse(self.transport.connected)
        self.assertIs(self.proto._rcvd_line, None)

    def test_clear_line_buffer(self):
        self.proto.dataReceived(self.S)
        self.assertEqual(self.proto.clearLineBuffer(), self.S)

    def test_send_line(self):
        self.proto.dataReceived(self.S + self.proto.delimiter)
        self.assertEqual(self.proto._rcvd_line, self.S.decode())

    def test_raw_data(self):
        clock = task.Clock()
        self.proto.callLater = clock.callLater
        self.proto.setRawMode()
        s = self.S + self.proto.delimiter
        self.proto.dataReceived(s)
        self.assertEqual(self.proto._rcvd_data, s)
        self.proto._rcvd_line = None
        self.proto.setLineMode(s)
        clock.advance(1)
        self.assertEqual(self.proto._rcvd_line, self.S.decode())
        self.proto.dataReceived(s)
        self.assertEqual(self.proto._rcvd_line, self.S.decode())

    def test_sendline(self):
        self.proto.sendLine(self.S)
        value = self.transport.value()
        self.assertEqual(value, self.S + self.proto.delimiter)


class TestBaseRedisProtocol(unittest.TestCase):
    def setUp(self):
        self._protocol = redis.BaseRedisProtocol()

    def test_build_ping(self):
        s = self._protocol._build_command("PING")
        self.assertEqual(s, six.b('*1\r\n$4\r\nPING\r\n'))

class TestTimeout(unittest.TestCase):
    def test_connect_timeout(self):
        c = redis.Connection(host = "10.255.255.1",
            port = 8000, reconnect = False, connectTimeout = 5.0, replyTimeout = 10.0)
        return self.assertFailure(c, error.TimeoutError)

    @defer.inlineCallbacks
    def test_reply_timeout(self):
        factory = protocol.ServerFactory()
        factory.protocol = PingMocker
        handler = reactor.listenTCP(8000, factory)
        c = yield redis.Connection(host = "127.0.0.1",
            port = 8000, reconnect = False, connectTimeout = 1.0, replyTimeout = 1.0)
        pong = yield c.ping()
        self.failUnlessEqual(pong, "PONG")
        get_one = c.get("foobar")
        get_two = c.get("foobar")
        yield self.assertFailure(get_one, redis.TimeoutError)
        yield self.assertFailure(get_two, redis.TimeoutError)

        yield handler.stopListening()
        yield c.disconnect()

    @staticmethod
    def _delay(time):
        d = defer.Deferred()
        reactor.callLater(time, d.callback, None)
        return d

    @defer.inlineCallbacks
    def test_delayed_call(self):
        factory = protocol.ServerFactory()
        factory.buildProtocol = lambda addr: PingMocker(delay=2)
        handler = reactor.listenTCP(8000, factory)

        c = yield redis.Connection(host="localhost", port=8000, reconnect=False, replyTimeout=3)

        try:
            yield self._delay(2)
            pong = yield c.ping()
            self.assertEqual(pong, "PONG")
        finally:
            yield handler.stopListening()
            yield c.disconnect()

class PingMocker(redis.LineReceiver):
    def __init__(self, delay=0):
        self.delay = delay
        self.calls = []

    def lineReceived(self, line):
        if line == 'PING':
            self.calls.append(reactor.callLater(self.delay, self.transport.write, b"+PONG\r\n"))

    def connectionLost(self, reason):
        for call in self.calls:
            if call.active():
                call.cancel()
