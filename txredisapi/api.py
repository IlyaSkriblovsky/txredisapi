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

import operator
import functools
import collections
from twisted.internet import defer
from txredisapi.hashring import HashRing

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
            return getattr(self.__factory.connection, method)
        except:
            return self.__disconnected

    def __connection_lost(self, deferred):
        if self.__factory.size == 0:
            self.__task.stop()
            deferred.callback(True)

    def disconnect(self):
        self.__factory.continueTrying = 0
        for conn in self.__factory.pool:
            try:
                conn.transport.loseConnection()
            except:
                pass

        d = defer.Deferred()
        self.__task = task.LoopingCall(self.__connection_lost, d)
        self.__task.start(1)
        return d

    def __repr__(self):
        try:
            cli = self.__factory.pool[0].transport.getPeer()
        except:
            info = "not connected"
        else:
            info = "%s:%s - %d connection(s)" % (cli.host, cli.port, self.__factory.size)
        return "<Redis: %s>" % info

class RedisShardingAPI(object):
    def __init__(self, connections):
        if isinstance(connections, defer.DeferredList):
            connections.addCallback(self.__makering)
        else:
            self.__ring = HashRing(connections)

    def __makering(self, results):
        connections = map(operator.itemgetter(1), results)
        self.__ring = HashRing(connections)
        return self

    def __wrap(self, m, *a, **kw):
        f = getattr(self.__ring(a[0]), m)
        return f(*a, **kw)
        
    def __getattr__(self, method):
        if method in [
            "get", "set", "getset",
            "incr", "decr", "exists",
            "delete", "get_type", "rename",
            "expire", "ttl", "push",
            "llen", "lrange", "ltrim",
            "lindex", "pop", "lset",
            "lrem", "sadd", "srem", 
            "sismember", "smembers", ]:
            return functools.partial(self.__wrap, method)

    @defer.inlineCallbacks
    def mget(self, *args):
        group = collections.defaultdict(lambda: [])
        for k in args:
            node = self.__ring(k)
            group[node].append(k)

        deferreds = []
        for node, keys in group.items():
            deferreds.append(node.mget(*keys))

        result = []
        response = yield defer.DeferredList(deferreds)
        for (ignore, values) in response:
            result += values
    
        defer.returnValue(result)
