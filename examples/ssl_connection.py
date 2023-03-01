#!/usr/bin/env python
import txredisapi as redis

from twisted.internet import defer, ssl
from twisted.internet import reactor


class TestContextFactory(ssl.ClientContextFactory):
  def getContext(self):
    ctx = ssl.ClientContextFactory.getContext(self)
    # ctx.load_verify_locations('./test/ca.crt')
    # ctx.use_certificate_file('./test/redis.crt')
    # ctx.use_privatekey_file('./test/redis.key')
    return ctx

@defer.inlineCallbacks
def main():
    rc = yield redis.Connection(ssl_context_factory=TestContextFactory())
    print(rc)

    yield rc.set("foo", "bar")
    v = yield rc.get("foo")
    print("foo:", repr(v))

    yield rc.disconnect()

if __name__ == "__main__":
    main().addCallback(lambda ign: reactor.stop())
    reactor.run()