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

import datetime

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from OpenSSL import crypto
from twisted.internet import defer, reactor, ssl
from twisted.trial import unittest

import txredisapi as redis

from tests.test_sentinel import FakeRedisFactory


_options = []


def selfSignedOptions():
    """
    Server side TLS options with a self-signed certificate, built once because
    generating a key is slow.

    twisted.internet.ssl.KeyPair.selfSignedCert() would be shorter, but it
    needs OpenSSL.crypto.X509Req, which recent pyOpenSSL releases no longer
    have.
    """
    if not _options:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name(
            [x509.NameAttribute(NameOID.COMMON_NAME, u"localhost")])
        now = datetime.datetime.now(datetime.timezone.utc)
        certificate = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(key.public_key())
            .serial_number(1)
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=1))
            .sign(key, hashes.SHA256())
        )
        _options.append(ssl.CertificateOptions(
            privateKey=crypto.PKey.from_cryptography_key(key),
            certificate=crypto.X509.from_cryptography(certificate)))

    return _options[0]


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
