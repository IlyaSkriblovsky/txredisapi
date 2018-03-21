import six
from mock import Mock
from twisted.internet import defer, reactor
from twisted.internet.protocol import Factory
from twisted.trial.unittest import TestCase

from txredisapi import BaseRedisProtocol, Sentinel, MasterNotFoundError


class FakeRedisProtocol(BaseRedisProtocol):
    @classmethod
    def _encode_value(cls, value):
        if value is None:
            return b'$-1\r\n'

        if isinstance(value, (list, tuple)):
            parts = [b'*', str(len(value)).encode('ascii'), b'\r\n']
            parts.extend(cls._encode_value(x) for x in value)
            return b''.join(parts)

        if isinstance(value, six.text_type):
            binary = value.encode("utf-8")
        elif isinstance(value, bytes):
            binary = value
        else:
            binary = six.text_type(value).encode("utf-8")
        return b''.join([b'$', str(len(binary)).encode('ascii'), b'\r\n', binary, b'\r\n'])

    def send_reply(self, reply):
        self.transport.write(self._encode_value(reply))

    def send_error(self, msg):
        self.transport.write(b''.join([b"-ERR ", msg.encode("utf-8"), b"\r\n"]))

    def replyReceived(self, request):
        if isinstance(request, list):
            if request[0] == "ROLE":
                self.send_reply(self.factory.role)

            else:
                self.send_error("Command not supported")
        else:
            BaseRedisProtocol.replyReceived(self, request)


class FakeAuthenticatedRedisProtocol(FakeRedisProtocol):
    def __init__(self, requirepass):
        FakeRedisProtocol.__init__(self)
        self.requirepass = requirepass
        self.authenticated = False

    def replyReceived(self, request):
        if isinstance(request, list):
            if request[0] == "AUTH":
                if request[1] == self.requirepass:
                    self.authenticated = True
                    self.send_reply("OK")
                else:
                    self.send_error("invalid password")
            else:
                if self.authenticated:
                    FakeRedisProtocol.replyReceived(self, request)
                else:
                    self.send_error("authentication required")
        else:
            FakeRedisProtocol.replyReceived(self, request)


class FakeSentinelProtocol(FakeRedisProtocol):
    def replyReceived(self, request):
        if request == ["SENTINEL", "MASTERS"]:
            host, port = self.factory.master_addr
            response = [["name", "test",
                         "ip", host,
                         "port", port,
                         "flags", self.factory.master_flags,
                         "num-other-sentinels", self.factory.num_other_sentinels]]
            self.send_reply(response)

        elif request[:2] == ["SENTINEL", "SLAVES"]:
            service_name = request[2]
            if service_name == self.factory.service_name:
                response = [
                    ["name", "{0}:{1}".format(host, port),
                     "ip", host,
                     "port", port,
                     "flags", flags,
                     "num-other-sentinels", self.factory.num_other_sentinels]
                    for (host, port), flags in zip(self.factory.slave_addrs, self.factory.slave_flags)
                ]
                self.send_reply(response)
            else:
                self.send_error("No such master with that name")

        else:
            FakeRedisProtocol.replyReceived(self, request)


class FakeRedisFactory(Factory):
    protocol = FakeRedisProtocol

    role = ["master", 0, ["127.0.0.1", 63791, 0]]


class FakeAuthenticatedRedisFactory(FakeRedisFactory):
    def __init__(self, requirepass):
        self.requirepass = requirepass

    def buildProtocol(self, addr):
        proto = FakeAuthenticatedRedisProtocol(self.requirepass)
        proto.factory = self
        return proto


class FakeSentinelFactory(FakeRedisFactory):
    protocol = FakeSentinelProtocol

    role = ["sentinel", ["test"]]

    service_name = "test"

    master_addr = ("127.0.0.1", 6379)
    master_flags = "master"
    slave_addrs = (("127.0.0.1", 63791), ("127.0.0.1", 63792))
    slave_flags = ("slave", "slave")

    num_other_sentinels = 0


class TestSentinelDiscovery(TestCase):

    sentinel_ports = [46379, 46380, 46381]

    def setUp(self):
        self.fake_sentinels = [FakeSentinelFactory() for _ in self.sentinel_ports]
        self.listeners = [reactor.listenTCP(port, fake_sentinel)
                          for fake_sentinel, port in zip(self.fake_sentinels, self.sentinel_ports)]

        self.client = Sentinel([("127.0.0.1", port) for port in self.sentinel_ports])
        self.client.discovery_timeout = 1

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.client.disconnect()
        for listener in self.listeners:
            listener.stopListening()

    @defer.inlineCallbacks
    def test_master(self):
        addr = yield self.client.discover_master("test")
        self.assertEqual(addr, FakeSentinelFactory.master_addr)

    @defer.inlineCallbacks
    def test_master_invalid_name(self):
        with self.assertRaises(MasterNotFoundError):
            yield self.client.discover_master("invalid")

    @defer.inlineCallbacks
    def test_master_fail_one_sentinel(self):
        # If a sentinel says it's master is down, discover should
        # be successful using another sentinels
        self.fake_sentinels[0].master_flags = "master,s_down"
        yield self.client.discover_master("test")

    @defer.inlineCallbacks
    def test_master_fail_all_sentinels(self):
        # If all sentinels claim the master is down, discover should fail
        for sentinel in self.fake_sentinels:
            sentinel.master_flags = "master,s_down"

        with self.assertRaises(MasterNotFoundError):
            yield self.client.discover_master("test")

    @defer.inlineCallbacks
    def test_master_no_min_sentinels(self):
        # Obey responses only from sentinels that talk to >= min_other_sentinels
        self.client.min_other_sentinels = 2
        with self.assertRaises(MasterNotFoundError):
            yield self.client.discover_master("test")

        for i, sentinel in enumerate(self.fake_sentinels):
            sentinel.num_other_sentinels = i
            sentinel.master_addr = (sentinel.master_addr, i)

        addr = yield self.client.discover_master("test")
        self.assertEqual(addr[1], 2)


    @defer.inlineCallbacks
    def test_slaves(self):
        addrs = yield self.client.discover_slaves("test")
        self.assertEqual(addrs, list(FakeSentinelFactory.slave_addrs))

    @defer.inlineCallbacks
    def test_slaves_invalid_name(self):
        addrs = yield self.client.discover_slaves("invalid")
        self.assertEqual(addrs, [])

    @defer.inlineCallbacks
    def test_slaves_fail_one_sentinel(self):
        self.fake_sentinels[0].slave_flags = ["slave,s_down", "slave,s_down"]
        yield self.client.discover_slaves("test")

    @defer.inlineCallbacks
    def test_slaves_fail_all_sentinels(self):
        for sentinel in self.fake_sentinels:
            sentinel.slave_flags = ["slave,s_down", "slave,s_down"]

        addrs = yield self.client.discover_slaves("test")
        self.assertEqual(addrs, [])


class TestConnectViaSentinel(TestCase):

    master_port = 36379
    slave_port = 36380
    sentinel_port = 46379

    def setUp(self):
        self.fake_master = FakeRedisFactory()
        self.master_listener = reactor.listenTCP(self.master_port, self.fake_master)
        self.fake_slave = FakeRedisFactory()
        self.fake_slave.role = ["slave", "127.0.0.1", self.master_port, "connected", 0]
        self.slave_listener = reactor.listenTCP(self.slave_port, self.fake_slave)

        self.fake_sentinel = FakeSentinelFactory()
        self.fake_sentinel.master_addr = ("127.0.0.1", self.master_port)
        self.fake_sentinel.slave_addrs = [("127.0.0.1", self.slave_port)]
        self.fake_sentinel.slave_flags = ["slave"]
        self.sentinel_listener = reactor.listenTCP(self.sentinel_port, self.fake_sentinel)

        self.client = Sentinel([("127.0.0.1", self.sentinel_port)])
        self.client.discovery_timeout = 1

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.client.disconnect()
        self.sentinel_listener.stopListening()
        self.master_listener.stopListening()
        self.slave_listener.stopListening()

    @defer.inlineCallbacks
    def test_master(self):
        conn = self.client.master_for("test")
        reply = yield conn.role()
        self.assertEqual(reply[0], "master")
        yield conn.disconnect()

    @defer.inlineCallbacks
    def test_retry_on_error(self):
        self.client.discover_master = Mock(side_effect=[defer.fail(MasterNotFoundError()),
                                                        defer.fail(MasterNotFoundError()),
                                                        defer.succeed(self.fake_sentinel.master_addr)])
        conn = self.client.master_for("test")
        yield conn.role()
        self.assertEqual(self.client.discover_master.call_count, 3)
        yield conn.disconnect()

    @defer.inlineCallbacks
    def test_retry_unexpected_role(self):
        self.fake_master.role = ["slave", "127.0.0.1", self.slave_port, "connected", 0]

        def side_effect(*args, **kwargs):
            if self.client.discover_master.call_count > 2:
                self.fake_master.role = FakeRedisFactory.role
            return defer.succeed(self.fake_sentinel.master_addr)

        self.client.discover_master = Mock(side_effect=side_effect)
        conn = self.client.master_for("test")
        reply = yield conn.role()
        self.assertEqual(reply[0], "master")
        self.assertEqual(self.client.discover_master.call_count, 3)
        yield conn.disconnect()

    @defer.inlineCallbacks
    def test_slave(self):
        conn = self.client.slave_for("test")
        reply = yield conn.role()
        self.assertEqual(reply[0], "slave")
        yield conn.disconnect()

    @defer.inlineCallbacks
    def test_fallback_to_master_if_no_slaves(self):
        self.client.discover_slaves = Mock(return_value=defer.succeed([]))
        conn = self.client.slave_for("test")
        reply = yield conn.role()
        self.assertEqual(reply[0], "master")
        yield conn.disconnect()

    @staticmethod
    def _delay(secs):
        d = defer.Deferred()
        reactor.callLater(secs, d.callback, None)
        return d

    @defer.inlineCallbacks
    def test_drop_all_when_master_changes(self):
        # When master address change detected, factory should drop and reestablish
        # all its connections

        conn = self.client.master_for("test", poolsize=3)
        yield conn.role()  # wait for connection

        addrs = [proto.transport.getPeer() for proto in conn._factory.pool]
        self.assertTrue(len(addrs), 3)
        self.assertTrue(all(addr.port == self.master_port for addr in addrs))

        # Change master address at sentinel and change role of the slave to master
        self.fake_sentinel.master_addr = ("127.0.0.1", self.slave_port)
        self.fake_slave.role = ["master", 0, ["127.0.0.1", self.slave_port, 0]]

        # Force reconnection of one connection
        conn._factory.pool[0].transport.loseConnection()

        # After a short time all connections should be to the new master
        yield self._delay(0.2)
        addrs = [proto.transport.getPeer() for proto in conn._factory.pool]
        self.assertTrue(len(addrs), 3)
        self.assertTrue(all(addr.port == self.slave_port for addr in addrs))

        yield conn.disconnect()


class TestAuthViaSentinel(TestCase):
    master_port = 36379
    sentinel_port = 46379

    def setUp(self):
        self.fake_master = FakeAuthenticatedRedisFactory('secret!')
        self.master_listener = reactor.listenTCP(self.master_port, self.fake_master)

        self.fake_sentinel = FakeSentinelFactory()
        self.fake_sentinel.master_addr = ("127.0.0.1", self.master_port)
        self.fake_sentinel.slave_addrs = []
        self.fake_sentinel.slave_flags = []
        self.sentinel_listener = reactor.listenTCP(self.sentinel_port, self.fake_sentinel)

        self.client = Sentinel([("127.0.0.1", self.sentinel_port)])
        self.client.discovery_timeout = 1

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.client.disconnect()
        self.sentinel_listener.stopListening()
        self.master_listener.stopListening()


    @defer.inlineCallbacks
    def test_auth(self):
        conn = self.client.master_for("test", password='secret!')
        reply = yield conn.role()
        self.assertEqual(reply[0], "master")
        yield conn.disconnect()
