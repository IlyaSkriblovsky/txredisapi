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


class MockLogger(object):
    def warning(self, *args, **kwargs):
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
    def test_lazy_connect_timeout(self):
        c = redis.lazyConnection(host='10.255.255.1',
            port=8000, reconnect=True, connectTimeout=0.0, replyTimeout=10.0)
        c._factory.maxRetries = 0
        p = c.ping()

        yield self.assertFailure(p, error.TimeoutError)
        yield c.disconnect()

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

        yield c.disconnect()
        yield handler.stopListening()

    @staticmethod
    def _delay(time):
        d = defer.Deferred()
        reactor.callLater(time, d.callback, None)
        return d

    @defer.inlineCallbacks
    def test_delayed_call(self):
        mockers = []

        def capture_mocker(*args, **kwargs):
            m = PingMocker(*args, **kwargs)
            mockers.append(m)
            return m

        factory = protocol.ServerFactory()
        factory.buildProtocol = lambda addr: capture_mocker(delay=2)
        handler = reactor.listenTCP(8000, factory)
        c = yield redis.Connection(host="localhost", port=8000, reconnect=False, replyTimeout=3)

        # first ping should succeed, since 2 < 3
        yield self._delay(2)
        pong = yield c.ping()
        self.assertEqual(pong, "PONG")

        # kill the connection, and send it some pings
        for m in mockers:
            m.pause()

        # send out a few pings, on a "dead" connection
        pings = []

        for i in range(4):
            p = c.ping()
            pings.append(p)
            yield self._delay(1)

        # each should have failed (TBD: figure out which exact failures for each one)
        for i, p in enumerate(pings):
            yield self.assertFailure(p, redis.ConnectionError)

        for m in mockers:
            m.unpause()

        yield c.disconnect()
        yield handler.stopListening()

    @defer.inlineCallbacks
    def test_delayed_call_reconnect(self):
        mockers = []

        def capture_mocker(*args, **kwargs):
            m = PingMocker(*args, **kwargs)
            mockers.append(m)
            return m
        factory = protocol.ServerFactory()
        factory.buildProtocol = lambda addr: capture_mocker(delay=2)
        handler = reactor.listenTCP(8001, factory)
        c = yield redis.Connection(host="localhost", port=8001, reconnect=True, replyTimeout=3)

        # pause the server
        for m in mockers:
            m.pause()

        c._factory.continueTrying = 0

        # send out a ping on a dead connection
        yield self.assertFailure(c.ping(), redis.ConnectionError)

        c._factory.continueTrying = 1

        for m in mockers:
            m.unpause()

        pong = yield c.ping()
        self.assertEqual(pong, "PONG")

        yield c.disconnect()
        yield handler.stopListening()


    @defer.inlineCallbacks
    def test_connection_lost(self):
        mockers = []

        def capture_mocker(*args, **kwargs):
            m = PingMocker(*args, **kwargs)
            mockers.append(m)
            return m

        factory = protocol.ServerFactory()
        factory.buildProtocol = lambda addr: capture_mocker(delay=0)
        handler = reactor.listenTCP(8002, factory)
        c = yield redis.Connection(host="localhost", port=8002, reconnect=False, replyTimeout=3,  logger=MockLogger())

        pong = yield c.ping()
        self.assertEqual(pong, "PONG")

        self.assertEquals(str(c), "<Redis Connection: 127.0.0.1:8002 - 1 connection(s)>")

        for m in mockers:
            yield m.transport.loseConnection()

        # yield handler.stopListening()
        yield self._delay(1.0)

        self.assertEquals(str(c), "<Redis Connection: Not connected>")

        yield self.assertFailure(c.ping(), redis.ConnectionError)

        c = yield redis.Connection(host="localhost", port=8002, reconnect=False, replyTimeout=3, logger=MockLogger())
        pong = yield c.ping()
        self.assertEqual(pong, "PONG")

        yield c.disconnect()
        yield handler.stopListening()

class PingMocker(redis.LineReceiver):
    def __init__(self, delay=0):
        self.delay = delay
        self.calls = []
        self.paused = False

    def pause(self):
        self.paused = True

    def unpause(self):
        self.paused = False

    @defer.inlineCallbacks
    def _pong(self):
        while self.paused:
            yield self._delay(0.01)

        self.transport.write(b"+PONG\r\n")

    def lineReceived(self, line):
        if line == 'PING':
            self.calls.append(reactor.callLater(self.delay, self._pong))

    def connectionLost(self, reason):
        for call in self.calls:
            if call.active():
                call.cancel()
