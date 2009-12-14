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

from txredis.protocol import RedisProtocol
from twisted.internet import task, defer, reactor, protocol

class RedisAPI(object):
    def __init__(self, factory):
        self.__factory = factory
        self._connected = factory.deferred

    def __disconnected(self, *args, **kwargs):
        deferred = defer.Deferred()
        deferred.errback(RuntimeWarning("not connected"))
        return deferred

    def __getattr__(self, method):
        try:
            assert self.__factory.size
            conn = self.__factory.pool[self.__factory.id % self.__factory.size]
            function = getattr(conn, method)
            self.__factory.id += 1
        except:
            return self.__disconnected
        return function

    def __connection_lost(self, deferred):
        if self.__factory.size == 0:
            self.__job.stop()
            deferred.callback(True)

    def disconnect(self):
        self.__factory.continueTrying = 0
        for conn in self.__factory.pool:
            try:
                conn.transport.loseConnection()
            except:
                pass

        d = defer.Deferred()
        self.__job = task.LoopingCall(self.__connection_lost, d)
        self.__job.start(1)
        return d

    def __repr__(self):
        try:
            cli = self.__factory.pool[0].transport.getPeer()
        except:
            info = "not connected"
        else:
            info = "%s:%s - %d connection(s)" % (cli.host, cli.port, self.__factory.size)
        return "<Redis: %s>" % info


class _RedisFactory(protocol.ReconnectingClientFactory):
    maxDelay = 10
    protocol = RedisProtocol

    def __init__(self, pool_size):
        self.id = 0
        self.size = 0
        self.pool = []
        self.pool_size = pool_size
        self.deferred = defer.Deferred()
        self.API = RedisAPI(self)

    def append(self, conn):
        self.pool.append(conn)
        self.size += 1
        if self.deferred and self.size == self.pool_size:
            self.deferred.callback(self.API)
            self.deferred = None

    def remove(self, conn):
        try:
            self.pool.remove(conn)
        except:
            pass
        self.size = len(self.pool)

def _Connection(host, port, reconnect, pool_size, lazy):
    factory = _RedisFactory(pool_size)
    factory.continueTrying = reconnect
    for x in xrange(pool_size):
        reactor.connectTCP(host, port, factory)
    return (lazy is True) and factory.API or factory.deferred

def RedisConnection(host="localhost", port=6379, reconnect=True):
    return _Connection(host, port, reconnect, pool_size=1, lazy=False)

def lazyRedisConnection(host="localhost", port=6379, reconnect=True):
    return _Connection(host, port, reconnect, pool_size=1, lazy=True)

def RedisConnectionPool(host="localhost", port=6379, reconnect=True, pool_size=5):
    return _Connection(host, port, reconnect, pool_size, lazy=False)

def lazyRedisConnectionPool(host="localhost", port=6379, reconnect=True, pool_size=5):
    return _Connection(host, port, reconnect, pool_size, lazy=True)
