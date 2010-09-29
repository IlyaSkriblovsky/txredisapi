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

import sys
import types
import operator
import functools
import collections
from twisted.internet import task, defer
from txredisapi.hashring import HashRing
import re

from txredisapi import protocol

_findhash = re.compile('.+\{(.*)\}.*', re.I)

class RedisAPI(object):
    def __init__(self, factory):
        self._factory = factory
        self._connected = factory.deferred

    def __disconnected(self, *args, **kwargs):
        deferred = defer.Deferred()
        deferred.errback(RuntimeWarning("not connected"))
        return deferred

    def __getattr__(self, method):
        try:
            return getattr(self._factory.connection, method)
        except:
            return self.__disconnected

    def __wait_pool_cleanup(self, deferred):
        if self._factory.size == 0:
            self.__task.stop()
            deferred.callback(True)

    def disconnect(self):
        self._factory.continueTrying = 0
        for conn in self._factory.pool:
            try:
                conn.transport.loseConnection()
            except:
                pass

        d = defer.Deferred()
        self.__task = task.LoopingCall(self.__wait_pool_cleanup, d)
        self.__task.start(1)
        return d

    def __repr__(self):
        try:
            cli = self._factory.pool[0].transport.getPeer()
        except:
            info = "not connected"
            return info
        else:
            info = "%s:%s - %d connection(s)" % (cli.host, cli.port, self._factory.size)
            return info

class RedisShardingAPI(object):
    def __init__(self, connections):
        if isinstance(connections, defer.DeferredList):
            connections.addCallback(self.__makering)
        else:
            self.__ring = HashRing(connections)

    @defer.inlineCallbacks
    def disconnect(self):
        for conn in self.__ring.nodes:
            yield conn.disconnect()
        defer.returnValue(True)
        
    def __makering(self, results):
        connections = map(operator.itemgetter(1), results)
        self.__ring = HashRing(connections)
        return self

    def __wrap(self, method, *args, **kwargs):
        try:
            key = args[0]
            assert isinstance(key, types.StringTypes)
        except:
            raise ValueError("method '%s' requires a key as the first argument" % method)

        g = _findhash.match(key)

        if g != None and len(g.groups()) > 0:
            node = self.__ring(g.groups()[0])
        else:
            node = self.__ring(key)

        #print "node for '%s' is: %s" % (key, node)
        f = getattr(node, method)
        return f(*args, **kwargs)
        
    def __getattr__(self, method):
        if method in [
            "get", "set", "getset",
            "incr", "decr", "exists",
            "delete", "get_type", "rename",
            "expire", "ttl", "push",
            "llen", "lrange", "ltrim",
            "lindex", "pop", "lset",
            "lrem", "sadd", "srem", 
            "sismember", "smembers", 
            "zadd", "zrem", "zincr",
            "zrange", "zrevrange", "zrangebyscore",
            "zremrangebyscore", "zcard", "zscore", 
            "hget", "hset", "hdel", "hincrby", "hlen", 
            "hkeys", "hvals", "hgetall", "hexists", "hmget", "hmset", 
            "publish",
            ]:
            return functools.partial(self.__wrap, method)
        else:
            raise NotImplementedError("method '%s' cannot be sharded" % method)

    @defer.inlineCallbacks
    def mget(self, keys, *args):
        """
        high-level mget, required because of the sharding support
        """

        keys = protocol.list_or_args("mget", keys, args)
        group = collections.defaultdict(lambda: [])
        for k in keys:
            node = self.__ring(k)
            group[node].append(k)

        deferreds = []
        for node, keys in group.items():
            nd=node.mget(keys)
            deferreds.append(nd)

        result = []
        response = yield defer.DeferredList(deferreds)
        for (success, values) in response:
            if success:
                result += values

        defer.returnValue(result)

    def __repr__(self):
        nodes = []
        for conn in self.__ring.nodes:
            try:
                cli = conn._factory.pool[0].transport.getPeer()
            except:
                pass
            else:
                nodes.append("%s:%s/%d" % (cli.host, cli.port, conn._factory.size))
        return "<RedisSharding: %s>" % ", ".join(nodes)
