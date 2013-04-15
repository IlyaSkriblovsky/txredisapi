import txredisapi as redis
from twisted.internet import defer, reactor
from twisted.trial import unittest

redis_host = "localhost"
redis_port = 6379

class TestSubscriberProtocol(unittest.TestCase):
    @defer.inlineCallbacks
    def setUp(self):
        factory = redis.SubscriberFactory()
        factory.continueTrying = False
        reactor.connectTCP(redis_host, redis_port, factory)
        self.db = yield factory.deferred

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.db.disconnect()

    @defer.inlineCallbacks
    def testDisconnectErrors(self):
        # Slightly dirty, but we want a reference to the actual
        # protocol instance
        conn = yield self.db._factory.getConnection(True)

        # This should return a deferred from the replyQueue; then
        # loseConnection will make it do an errback with a
        # ConnectionError instance
        d = self.db.subscribe('foo')

        conn.transport.loseConnection()
        try:
            yield d
            self.fail()
        except redis.ConnectionError:
            pass

        # This should immediately errback with a ConnectionError
        # instance when getConnection finds 0 active instances in the
        # factory
        try:
            yield self.db.subscribe('bar')
            self.fail()
        except redis.ConnectionError:
            pass

        # This should immediately raise a ConnectionError instance
        # when execute_command() finds that the connection is not
        # connected
        try:
            yield conn.subscribe('baz')
            self.fail()
        except redis.ConnectionError:
            pass

    @defer.inlineCallbacks
    def testSubscribe(self):
        reply = yield self.db.subscribe("test_subscribe1")
        self.assertEqual(reply, [u"subscribe", u"test_subscribe1", 1])

        reply = yield self.db.subscribe("test_subscribe2")
        self.assertEqual(reply, [u"subscribe", u"test_subscribe2", 2])

    @defer.inlineCallbacks
    def testUnsubscribe(self):
        yield self.db.subscribe("test_unsubscribe1")
        yield self.db.subscribe("test_unsubscribe2")

        reply = yield self.db.unsubscribe("test_unsubscribe1")
        self.assertEqual(reply, [u"unsubscribe", u"test_unsubscribe1", 1])
        reply = yield self.db.unsubscribe("test_unsubscribe2")
        self.assertEqual(reply, [u"unsubscribe", u"test_unsubscribe2", 0])

    @defer.inlineCallbacks
    def testPSubscribe(self):
        reply = yield self.db.psubscribe("test_psubscribe1.*")
        self.assertEqual(reply, [u"psubscribe", u"test_psubscribe1.*", 1])

        reply = yield self.db.psubscribe("test_psubscribe2.*")
        self.assertEqual(reply, [u"psubscribe", u"test_psubscribe2.*", 2])

    @defer.inlineCallbacks
    def testPUnsubscribe(self):
        yield self.db.psubscribe("test_punsubscribe1.*")
        yield self.db.psubscribe("test_punsubscribe2.*")

        reply = yield self.db.punsubscribe("test_punsubscribe1.*")
        self.assertEqual(reply, [u"punsubscribe", u"test_punsubscribe1.*", 1])
        reply = yield self.db.punsubscribe("test_punsubscribe2.*")
        self.assertEqual(reply, [u"punsubscribe", u"test_punsubscribe2.*", 0])
