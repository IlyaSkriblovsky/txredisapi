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
import txredisapi as redis
from twisted.trial import unittest
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
    S = 'TEST'

    def setUp(self):
        self.proto = LineReceiverSubclass()
        self.transport = StringTransportWithDisconnection()
        self.proto.makeConnection(self.transport)
        self.transport.protocol = self.proto
        self.proto.factory = MockFactory()

    def test_excess_line_length(self):
        self.assertTrue(self.transport.connected)
        self.proto.dataReceived('\x00' * (self.proto.MAX_LENGTH + 1))
        self.assertFalse(self.transport.connected)

    def test_excess_delimited_line(self):
        self.assertTrue(self.transport.connected)
        self.proto.dataReceived(self.S + self.proto.delimiter)
        self.assertEqual(self.proto._rcvd_line, self.S)
        s = ('\x00' * (self.proto.MAX_LENGTH + 1)) + self.proto.delimiter
        self.proto._rcvd_line = None
        self.proto.dataReceived(s)
        self.assertFalse(self.transport.connected)
        self.assertIsNone(self.proto._rcvd_line)

    def test_clear_line_buffer(self):
        self.proto.dataReceived(self.S)
        self.assertEqual(self.proto.clearLineBuffer(), self.S)

    def test_send_line(self):
        self.proto.dataReceived(self.S + self.proto.delimiter)
        self.assertEqual(self.proto._rcvd_line, self.S)

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
        self.assertEqual(self.proto._rcvd_line, self.S)
        self.proto.dataReceived(s)
        self.assertEqual(self.proto._rcvd_line, self.S)

    def test_sendline(self):
        self.proto.sendLine(self.S)
        self.assertEqual(self.transport.value(), self.S + self.proto.delimiter)
