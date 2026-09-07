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

from twisted.internet import defer, reactor, ssl
from twisted.trial import unittest

import txredisapi as redis

from tests.test_sentinel import FakeRedisFactory


_certificate = []


def selfSignedOptions():
    """
    Server side TLS options with a self-signed certificate, generated once
    because generating a key is slow.
    """
    if not _certificate:
        key = ssl.KeyPair.generate(size=2048)
        _certificate.append((key, key.selfSignedCert(1, CN="localhost")))

    key, cert = _certificate[0]
    return ssl.CertificateOptions(privateKey=key.original,
                                  certificate=cert.original)


class TestSSLConnections(unittest.TestCase):
    timeout = 15

    def setUp(self):
        self.listener = reactor.listenSSL(0, FakeRedisFactory(),
                                          selfSignedOptions(),
                                          interface="127.0.0.1")
        self.port = self.listener.getHost().port
        self.addCleanup(self.listener.stopListening)

    @defer.inlineCallbacks
    def test_context_factory(self):
        db = yield redis.Connection(
            "127.0.0.1", self.port, reconnect=False,
            ssl_context_factory=ssl.ClientContextFactory())
        self.addCleanup(db.disconnect)

        role = yield db.role()
        self.assertEqual(role[0], "master")

    @defer.inlineCallbacks
    def test_ssl_true(self):
        db = yield redis.Connection("127.0.0.1", self.port, reconnect=False,
                                    ssl_context_factory=True)
        self.addCleanup(db.disconnect)

        role = yield db.role()
        self.assertEqual(role[0], "master")

    @defer.inlineCallbacks
    def test_pool(self):
        db = yield redis.ConnectionPool("127.0.0.1", self.port, poolsize=2,
                                        reconnect=False,
                                        ssl_context_factory=True)
        self.addCleanup(db.disconnect)

        role = yield db.role()
        self.assertEqual(role[0], "master")
