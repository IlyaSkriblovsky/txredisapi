#!/usr/bin/env python
# coding: utf-8

import txredisapi
from twisted.internet import defer, reactor

@defer.inlineCallbacks
def main():
    conn = yield txredisapi.RedisShardingConnection(["localhost:6379"])
    print conn
    #conn = yield txredisapi.RedisShardingConnection(("10.0.0.1:6379", "10.0.0.2:6379"))
    #conn = yield txredisapi.RedisShardingConnectionPool(("10.0.0.1:6379", "10.0.0.2:6379"))

    keys = ["test:%d" % x for x in xrange(100)]
    for k in keys:
        yield conn.set(k, "foobar")

    result = yield conn.mget(*keys)
    print result

if __name__ == "__main__":
    main().addCallback(lambda ign: reactor.stop())
    reactor.run()
