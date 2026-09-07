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

from twisted.internet import defer, reactor
from twisted.internet.address import IPv6Address
from twisted.internet.error import CannotListenError
from twisted.internet.interfaces import IHostnameResolver, IHostResolution
from twisted.trial import unittest
from zope.interface import implementer

import txredisapi as redis

from tests.test_sentinel import FakeRedisFactory


@implementer(IHostResolution)
class Resolution(object):
    def __init__(self, name):
        self.name = name

    def cancel(self):
        pass


@implementer(IHostnameResolver)
class IPv6OnlyResolver(object):
    """
    Resolves a single hostname to a single IPv6 address and knows nothing
    else, just like a name that has an AAAA record and no A record.
    """

    def __init__(self, hostname, address="::1"):
        self.hostname = hostname
        self.address = address

    def resolveHostName(self, resolutionReceiver, hostName, portNumber=0,
                        addressTypes=None, transportSemantics="TCP"):
        resolution = Resolution(hostName)
        resolutionReceiver.resolutionBegan(resolution)
        if hostName == self.hostname and \
                (addressTypes is None or IPv6Address in addressTypes):
            resolutionReceiver.addressResolved(
                IPv6Address("TCP", self.address, portNumber))
        resolutionReceiver.resolutionComplete()
        return resolution


class TestIPv6Connections(unittest.TestCase):
    """
    Servers reachable over IPv6 only must be reachable as well: names are
    resolved with getaddrinfo(), not with the IPv4-only gethostbyname().
    """

    hostname = "ipv6-only.invalid"

    # connecting to the loopback is instant; don't sit through trial's default
    # timeout if it ever stops working
    timeout = 15

    def setUp(self):
        try:
            self.listener = reactor.listenTCP(0, FakeRedisFactory(),
                                              interface="::1")
        except CannotListenError:
            raise unittest.SkipTest("no IPv6 loopback available")

        self.port = self.listener.getHost().port
        self.addCleanup(self.listener.stopListening)

    @defer.inlineCallbacks
    def test_aaaa_only_hostname(self):
        previous = reactor.installNameResolver(IPv6OnlyResolver(self.hostname))
        self.addCleanup(reactor.installNameResolver, previous)

        db = yield redis.Connection(self.hostname, self.port, reconnect=False)
        self.addCleanup(db.disconnect)

        role = yield db.role()
        self.assertEqual(role[0], "master")

    @defer.inlineCallbacks
    def test_ipv6_literal(self):
        db = yield redis.Connection("::1", self.port, reconnect=False)
        self.addCleanup(db.disconnect)

        role = yield db.role()
        self.assertEqual(role[0], "master")
