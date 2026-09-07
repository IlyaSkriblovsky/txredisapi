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

from __future__ import print_function

import txredisapi as redis

from twisted.application.internet import ClientService, backoffPolicy
from twisted.application import service
from twisted.internet import reactor
from twisted.internet.endpoints import HostnameEndpoint


class MyProtocol(redis.SubscriberProtocol):
    def connectionMade(self):
        print("waiting for messages...")
        print("use the redis client to send messages:")
        print("$ redis-cli publish zz test")
        print("$ redis-cli publish foo.bar hello world")

        #self.auth("foobared")

        self.subscribe("zz")
        self.psubscribe("foo.*")
        # reactor.callLater(10, self.unsubscribe, "zz")
        # reactor.callLater(15, self.punsubscribe, "foo.*")

        # self.factory.stopTrying()
        # self.transport.loseConnection()

    def messageReceived(self, pattern, channel, message):
        print("pattern=%s, channel=%s message=%s" % (pattern, channel, message))

    def connectionLost(self, reason):
        print("lost connection:", reason)


class MyFactory(redis.SubscriberFactory):
    protocol = MyProtocol


application = service.Application("subscriber")
endpoint = HostnameEndpoint(reactor, "127.0.0.1", 6379)
srv = ClientService(endpoint, MyFactory(),
                    retryPolicy=backoffPolicy(maxDelay=120))
srv.setServiceParent(application)
