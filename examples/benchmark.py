# coding: utf-8
from __future__ import print_function

import time
from twisted.internet import defer
from twisted.internet import reactor
import txredisapi as redis

HOST = 'localhost'
PORT = 6379

N = 1000


@defer.inlineCallbacks
def test_setget():
    key = 'test'
    conn = yield redis.Connection(HOST, PORT)
    start = time.time()
    for i in xrange(N):
        yield conn.set(key, 'test_data')
        yield conn.get(key)
    print("done set-get: %.4fs." % ((time.time() - start) / N))


@defer.inlineCallbacks
def test_lrange():
    key = 'test_list'
    list_length = 1000
    conn = yield redis.Connection(HOST, PORT)
    yield defer.DeferredList([conn.lpush(key, str(i)) for i in xrange(list_length)])
    start = time.time()
    for i in xrange(N):
        yield conn.lrange(key, 0, 999)
    print("done lrange: %.4fs." % ((time.time() - start) / N))


@defer.inlineCallbacks
def run():
    yield test_setget()
    yield test_lrange()
    reactor.stop()

run()
reactor.run()
