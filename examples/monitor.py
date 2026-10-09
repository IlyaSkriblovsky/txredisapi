#!/usr/bin/env twistd -ny
# coding: utf-8
# Copyright 2012 Gleicon Moraes/Alexandre Fiori
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
# run: twistd -ny monitor.tac
# it takes the full connection so no extra commands can be issued

from __future__ import print_function

import txredisapi

from twisted.application import service
from twisted.internet import reactor
from twisted.internet.endpoints import HostnameEndpoint


class MyMonitor(txredisapi.MonitorProtocol):
    def connectionMade(self):
        print("waiting for monitor data")
        print("use the redis client to send commands in another terminal")
        self.monitor()

    def messageReceived(self, message):
        print(">> %s" % message)

    def connectionLost(self, reason):
        print("lost connection:", reason)


class MyFactory(txredisapi.MonitorFactory):
    protocol = MyMonitor


class RedisService(service.Service):
    """
    Keeps the connection up for as long as the application runs. The factory
    reconnects and backs off on its own; disconnect() stops it for good.
    """

    def startService(self):
        self.factory = MyFactory()
        self.factory.startConnecting(
            HostnameEndpoint(reactor, "127.0.0.1", 6379))

    def stopService(self):
        return self.factory.disconnect()


application = service.Application("monitor")
RedisService().setServiceParent(application)
