#!/usr/bin/env twistd -ny
# coding: utf-8
# Copyright 2009 Alexandre Fiori
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
#
# See the PUBSUB documentation for details:
# http://code.google.com/p/redis/wiki/PublishSubscribe
#
# run: twistd -ny subscriber.tac
# You may not use regular commands (like get, set, etc...) on the
# subscriber connection.

import txredisapi as redis

from twisted.application import internet
from twisted.application import service
from twisted.internet import reactor


class myProtocol(redis.SubscriberProtocol):
    def connectionMade(self):
        print "waiting for messages..."
        print "use the redis client to send messages:"
        print "$ redis-cli publish zz test"
        print "$ redis-cli publish foo.bar hello world"
        self.subscribe("zz")
        self.psubscribe("foo.*")
        #reactor.callLater(10, self.unsubscribe, "zz")
        #reactor.callLater(15, self.punsubscribe, "foo.*")

        # self.continueTrying = False
        # self.transport.loseConnection()

    def messageReceived(self, pattern, channel, message):
        print "pattern=%s, channel=%s message=%s" % (pattern, channel, message)

    def connectionLost(self, reason):
        print "lost connection:", reason


class myFactory(redis.SubscriberFactory):
    # SubscriberFactory is a wapper for the ReconnectingClientFactory
    maxDelay = 120
    continueTrying = True
    protocol = myProtocol


application = service.Application("subscriber")
srv = internet.TCPClient("127.0.0.1", 6379, myFactory())
srv.setServiceParent(application)
