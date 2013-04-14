import txredisapi as redis
from twisted.internet import defer
from twisted.trial import unittest

redis_host = "localhost"
redis_port = 6379

class TestBlockingCommands(unittest.TestCase):
    QUEUE_KEY = 'txredisapi:test_queue'
    TEST_KEY = 'txredisapi:test_key'
    QUEUE_VALUE = 'queue_value'

    @defer.inlineCallbacks
    def testBlocking(self):
        db = yield redis.ConnectionPool(redis_host, redis_port, poolsize=2,
                                        reconnect=False)
        yield db.delete(self.QUEUE_KEY, self.TEST_KEY)

        # Block first connection.
        d = db.brpop(self.QUEUE_KEY, timeout=3)
        # Use second connection.
        yield db.set(self.TEST_KEY, 'somevalue')
        # Should use second connection again. Will block till end of
        # brpop otherwise.
        yield db.lpush('txredisapi:test_queue', self.QUEUE_VALUE)

        brpop_result = yield d
        self.assertNotEqual(brpop_result, None)

        yield db.delete(self.QUEUE_KEY, self.TEST_KEY)
        yield db.disconnect()


