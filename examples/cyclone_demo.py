#!/usr/bin/env python
# coding: utf-8
#
# Copyright 2010 Alexandre Fiori
# based on the original Tornado by Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import collections
import functools
import os.path
import sys

import cyclone.web
import cyclone.redis

from twisted.python import log
from twisted.internet import defer, reactor


class Application(cyclone.web.Application):
    def __init__(self):
        handlers = [
            (r"/text/(.+)", TextHandler),
            (r"/queue/(.+)", QueueHandler),
        ]
        settings = dict(
            debug=True,
            static_path="./frontend/static",
            template_path="./frontend/template",
        )
        RedisMixin.setup("127.0.0.1", 6379, 0, 10)
        cyclone.web.Application.__init__(self, handlers, **settings)


class RedisMixin(object):
    dbconn = None
    psconn = None
    channels = collections.defaultdict(lambda: [])

    @classmethod
    def setup(self, host, port, dbid, poolsize):
        # PubSub client connection
        qf = cyclone.redis.SubscriberFactory()
        qf.maxDelay = 20
        qf.protocol = QueueProtocol
        reactor.connectTCP(host, port, qf)

        # Normal client connection
        RedisMixin.dbconn = cyclone.redis.lazyConnectionPool(host, port,
                                                             dbid, poolsize)

    def subscribe(self, channel):
        if RedisMixin.psconn is None:
            raise cyclone.web.HTTPError(503) # Service Unavailable

        if channel not in RedisMixin.channels:
            log.msg("Subscribing entire server to %s" % channel)
            if "*" in channel:
                RedisMixin.psconn.psubscribe(channel)
            else:
                RedisMixin.psconn.subscribe(channel)

        RedisMixin.channels[channel].append(self)
        log.msg("Client %s subscribed to %s" % \
                (self.request.remote_ip, channel))

    def unsubscribe_all(self, ign):
        # Unsubscribe peer from all channels
        for channel, peers in RedisMixin.channels.iteritems():
            try:
                peers.pop(peers.index(self))
            except:
                continue

            log.msg("Client %s unsubscribed from %s" % \
                    (self.request.remote_ip, channel))

            # Unsubscribe from channel if no peers are listening
            if not len(peers) and RedisMixin.psconn is not None:
                log.msg("Unsubscribing entire server from %s" % channel)
                if "*" in channel:
                    RedisMixin.psconn.punsubscribe(channel)
                else:
                    RedisMixin.psconn.unsubscribe(channel)

    def broadcast(self, pattern, channel, message):
        peers = self.channels.get(pattern or channel)
        if not peers:
            return

        # Broadcast the message to all peers in channel
        for peer in peers:
            # peer is an HTTP client, RequestHandler
            peer.write("%s: %s\r\n" % (channel, message))
            peer.flush()


# Provide GET, SET and DELETE redis operations via HTTP
class TextHandler(cyclone.web.RequestHandler, RedisMixin):
    @defer.inlineCallbacks
    def get(self, key):
        try:
            value = yield self.dbconn.get(key)
        except Exception, e:
            log.err("Redis failed to get('%s'): %s" % (key, str(e)))
            raise cyclone.web.HTTPError(503)

        self.set_header("Content-Type", "text/plain")
        self.finish("%s=%s\r\n" % (key, value))

    @defer.inlineCallbacks
    def post(self, key):
        value = self.get_argument("value")
        try:
            yield self.dbconn.set(key, value)
        except Exception, e:
            log.err("Redis failed to set('%s', '%s'): %s" % (key, value, str(e)))
            raise cyclone.web.HTTPError(503)

        self.set_header("Content-Type", "text/plain")
        self.finish("%s=%s\r\n" % (key, value))

    @defer.inlineCallbacks
    def delete(self, key):
        try:
            n = yield self.dbconn.delete(key)
        except Exception, e:
            log.err("Redis failed to del('%s'): %s" % (key, str(e)))
            raise cyclone.web.HTTPError(503)

        self.set_header("Content-Type", "text/plain")
        self.finish("DEL %s=%d\r\n" % (key, n))


# GET will subscribe to channels or patterns
# POST will (obviously) post messages to channels
class QueueHandler(cyclone.web.RequestHandler, RedisMixin):
    @cyclone.web.asynchronous
    def get(self, channels):
        try:
            channels = channels.split(",")
        except Exception, e:
            log.err("Could not split channel names: %s" % str(e))
            raise cyclone.web.HTTPError(400, str(e))

        self.set_header("Content-Type", "text/plain")
        self.notifyFinish().addCallback(
            functools.partial(RedisMixin.unsubscribe_all, self))

        for channel in channels:
            self.subscribe(channel)
            self.write("subscribed to %s\r\n" % channel)
        self.flush()

    @defer.inlineCallbacks
    def post(self, channel):
        message = self.get_argument("message")

        try:
            n = yield self.dbconn.publish(channel, message.encode("utf-8"))
        except Exception, e:
            log.msg("Redis failed to publish('%s', '%s'): %s" % \
                    (channel, repr(message), str(e)))
            raise cyclone.web.HTTPError(503)

        self.set_header("Content-Type", "text/plain")
        self.finish("OK %d\r\n" % n)


class QueueProtocol(cyclone.redis.SubscriberProtocol, RedisMixin):
    def messageReceived(self, pattern, channel, message):
        # When new messages are published to Redis channels or patterns,
        # they are broadcasted to all HTTP clients subscribed to those
        # channels.
        RedisMixin.broadcast(self, pattern, channel, message)

    def connectionMade(self):
        RedisMixin.psconn = self

        # If we lost connection with Redis during operation, we
        # re-subscribe to all channels once the connection is re-established.
        for channel in self.channels:
            if "*" in channel:
                self.psubscribe(channel)
            else:
                self.subscribe(channel)

    def connectionLost(self, why):
        RedisMixin.psconn = None


def main():
    log.startLogging(sys.stdout)
    reactor.listenTCP(8888, Application(), interface="127.0.0.1")
    reactor.run()


if __name__ == "__main__":
    main()
