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
#
# Credits:
#   The Protocol class is an improvement of txRedis' protocol,
#   by Dorian Raymer and Ludovico Magnocavallo.
#
#   Sharding and Consistent Hashing implementation by Gleicon Moraes.
#

import bisect
import collections
import functools
import operator
import re
import warnings
import zlib

from itertools import imap

from twisted.internet import defer
from twisted.internet import protocol
from twisted.internet import reactor
from twisted.internet import task
from twisted.protocols import basic
from twisted.protocols import policies
from twisted.python import log

class RedisError(Exception):
    pass
class ConnectionError(RedisError):
    pass
class ResponseError(RedisError):
    pass
class InvalidResponse(RedisError):
    pass
class InvalidData(RedisError):
    pass

def list_or_args(command, keys, args):
    oldapi = bool(args)
    try:
        i = iter(keys)
        if isinstance(keys, (str, unicode)):
            raise TypeError
    except TypeError:
        oldapi = True
        keys = [keys]

    if oldapi:
        warnings.warn(DeprecationWarning(
            "Passing *args to redis.%s is deprecated. "
            "Pass an iterable to ``keys`` instead" % command))
        keys.extend(args)
    return keys

class RedisProtocol(basic.LineReceiver, policies.TimeoutMixin):
    """
    Redis client protocol.
    """

    ERROR = "-"
    STATUS = "+"
    INTEGER = ":"
    BULK = "$"
    MULTI_BULK = "*"

    def __init__(self, charset="utf-8", errors="strict"):
        self.charset = charset
        self.errors = errors

        self.bulk_length = 0
        self.bulk_buffer = ""
        self.multi_bulk_length = 0
        self.multi_bulk_reply = []
        self.replyQueueLength = 0
        self.replyQueue = defer.DeferredQueue()

        self.transactions = 0
        self.inTransaction = False

    @defer.inlineCallbacks
    def connectionMade(self):
        try:
            response = yield self.select(self.factory.dbid)
            if isinstance(response, ResponseError):
                raise response
        except Exception, e:
            self.factory.continueTrying = False
            self.transport.loseConnection()

            msg = "Redis error: could not set dbid=%s: %s" % \
                  (self.factory.dbid, str(e))
            self.factory.connectionError(msg)
            if self.factory.isLazy:
                log.msg(msg)
            defer.returnValue(None)

        self.connected = 1
        self.factory.addConnection(self)

    def connectionLost(self, why):
        self.connected = 0
        self.factory.delConnection(self)
        basic.LineReceiver.connectionLost(self, why)
        while self.replyQueueLength < 0:
            self.replyReceived(ConnectionError("Lost connection"))

    def lineReceived(self, line):
        """
        Reply types:
          "-" error message
          "+" single line status reply
          ":" integer number (protocol level only?)
          "$" bulk data
          "*" multi-bulk data
        """
        if not line:
            return

        self.resetTimeout()
        token = line[0] # first byte indicates reply type
        data = line[1:]
        if token == self.ERROR:
            self.errorReceived(data)
        elif token == self.STATUS:
            self.statusReceived(data)
        elif token == self.INTEGER:
            self.integerReceived(data)
        elif token == self.BULK:
            try:
                self.bulk_length = long(data)
            except ValueError:
                self.replyReceived(InvalidResponse(
                 "Cannot convert data '%s' to integer" % data))
                return
            if self.bulk_length == -1:
                self.bulkDataReceived(None)
                return
            else:
                self.bulk_length += 2 # \r\n
                self.setRawMode()
        elif token == self.MULTI_BULK:
            try:
                self.multi_bulk_length = long(data)
            except (TypeError, ValueError):
                self.replyReceived(InvalidResponse(
                 "Cannot convert multi-response header '%s' to integer" % data))
                self.multi_bulk_length = 0
                return
            if self.multi_bulk_length == -1:
                self.multi_bulk_reply = None
                self.multiBulkDataReceived()
                return
            elif self.multi_bulk_length == 0:
                self.multiBulkDataReceived()

    def rawDataReceived(self, data):
        """
        Process and dispatch to bulkDataReceived.
        """
        if self.bulk_length:
            data, rest = data[:self.bulk_length], data[self.bulk_length:]
            self.bulk_length -= len(data)
        else:
            rest = ""

        self.bulk_buffer += data
        if self.bulk_length == 0:
            bulk_buffer = self.bulk_buffer[:-2]
            self.bulk_buffer = ""
            self.bulkDataReceived(bulk_buffer)
            self.setLineMode(extra=rest)

    def errorReceived(self, data):
        """
        Error from server.
        """
        reply = ResponseError(data[4:] if data[:4] == "ERR " else data)

        if self.multi_bulk_length:
            self.handleMultiBulkElement(reply)
        else:
            self.replyReceived(reply)

    def statusReceived(self, data):
        """
        Single line status should always be a string.
        """
        #if data == "none":
        #    reply = None # should this happen here in the client?
        #else:
        #    reply = data

        reply = data
        if reply == "QUEUED":
            self.transactions += 1
            self.replyReceived(reply)
            return

        if self.multi_bulk_length:
            self.handleMultiBulkElement(reply)
        else:
            self.replyReceived(reply)

    def integerReceived(self, data):
        """
        For handling integer replies.
        """
        try:
            reply = int(data)
        except ValueError:
            reply = InvalidResponse(
            "Cannot convert data '%s' to integer" % data)

        if self.multi_bulk_length:
            self.handleMultiBulkElement(reply)
        else:
            self.replyReceived(reply)

    def bulkDataReceived(self, data):
        """
        Receipt of a bulk data element.
        """
        if data is None:
            element = data
        else:
            try:
                element = int(data) if data.find('.') == -1 else float(data)
            except (ValueError):
                try:
                    element = data.decode(self.charset)
                except UnicodeDecodeError:
                    element = data
        if self.multi_bulk_length > 0:
            self.handleMultiBulkElement(element)
        else:
            self.replyReceived(element)

    def handleMultiBulkElement(self, element):
        if self.inTransaction:
            self.transactions -= 1
        self.multi_bulk_reply.append(element)
        self.multi_bulk_length = self.multi_bulk_length - 1

        if self.multi_bulk_length == 0:
            self.multiBulkDataReceived()

    def multiBulkDataReceived(self):
        """
        Receipt of list or set of bulk data elements.
        """
        if self.inTransaction and self.transactions == 0:
            self.inTransaction = False
        reply = self.multi_bulk_reply
        self.multi_bulk_reply = []
        self.multi_bulk_length = 0
        self.replyReceived(reply)

    def replyReceived(self, reply):
        """
        Complete reply received and ready to be pushed to the requesting
        function.
        """
        self.replyQueueLength += 1
        self.replyQueue.put(reply)

    def get_response(self):
        """return deferred which will fire with response from server.
        """
        self.replyQueueLength -= 1
        return self.replyQueue.get()

    def encode(self, s):
        if isinstance(s, str):
            return s
        if isinstance(s, unicode):
            try:
                return s.encode(self.charset, self.errors)
            except UnicodeEncodeError, e:
                raise InvalidData(
                "Error encoding unicode value '%s': %s" % (repr(s), e))
        return str(s)

    def execute_command(self, *args):
        if self.connected == 0:
            raise ConnectionError("Not connected")
        else:
            cmds = ["$%s\r\n%s\r\n" % (len(enc_value), enc_value)
                for enc_value in imap(self.encode, args)]
            self.transport.write("*%s\r\n%s" % (len(cmds), "".join(cmds)))
            return self.get_response()

    ##
    # REDIS COMMANDS
    ##

    # Connection handling
    def quit(self):
        """
        Close the connection
        """
        self.factory.continueTrying = False
        return self.execute_command("QUIT")

    def auth(self, password):
        """
        Simple password authentication if enabled
        """
        return self.execute_command("AUTH", password)

    def ping(self):
        """
        Ping the server
        """
        return self.execute_command("PING")

    # Commands operating on all value types
    def exists(self, key):
        """
        Test if a key exists
        """
        return self.execute_command("EXISTS", key)

    def delete(self, *keys):
        """
        Delete one or more keys
        """
        return self.execute_command("DEL", *keys)

    def type(self, key):
        """
        Return the type of the value stored at key
        """
        return self.execute_command("TYPE", key)

    def keys(self, pattern="*"):
        """
        Return all the keys matching a given pattern
        """
        return self.execute_command("KEYS", pattern)

    def randomkey(self):
        """
        Return a random key from the key space
        """
        return self.execute_command("RANDOMKEY")

    def rename(self, oldkey, newkey):
        """
        Rename the old key in the new one,
        destroying the newname key if it already exists
        """
        return self.execute_command("RENAME", oldkey, newkey)

    def renamenx(self, oldkey, newkey):
        """
        Rename the oldname key to newname,
        if the newname key does not already exist
        """
        return self.execute_command("RENAMENX", oldkey, newkey)

    def dbsize(self):
        """
        Return the number of keys in the current db
        """
        return self.execute_command("DBSIZE")

    def expire(self, key, time):
        """
        Set a time to live in seconds on a key
        """
        return self.execute_command("EXPIRE", key, time)

    def persist(self, key):
        """
        Remove the expire from a key
        """
        return self.execute_command("PERSIST", key)

    def ttl(self, key):
        """
        Get the time to live in seconds of a key
        """
        return self.execute_command("TTL", key)

    def select(self, index):
        """
        Select the DB with the specified index
        """
        return self.execute_command("SELECT", index)

    def move(self, key, dbindex):
        """
        Move the key from the currently selected DB to the dbindex DB
        """
        return self.execute_command("MOVE", key, dbindex)

    def flush(self, all_dbs=False):
        warnings.warn(DeprecationWarning(
            "redis.flush() has been deprecated, "
            "use redis.flush() or redis.flushall() instead"))
        return all_dbs and self.flushall() or self.flushdb()

    def flushdb(self):
        """
        Remove all the keys from the currently selected DB
        """
        return self.execute_command("FLUSHDB")

    def flushall(self):
        """
        Remove all the keys from all the databases
        """
        return self.execute_command("FLUSHALL")

    # Commands operating on string values
    def set(self, key, value, preserve=False, getset=False):
        """
        Set a key to a string value
        """
        if preserve:
            warnings.warn(DeprecationWarning(
                "preserve option to 'set' is deprecated, "
                "use redis.setnx() instead"))
            return self.setnx(key, value)

        if getset:
            warnings.warn(DeprecationWarning(
                "getset option to 'set' is deprecated, "
                "use redis.getset() instead"))
            return self.getset(key, value)

        return self.execute_command("SET", key, value)

    def get(self, key):
        """
        Return the string value of the key
        """
        return self.execute_command("GET", key)

    def getset(self, key, value):
        """
        Set a key to a string returning the old value of the key
        """
        return self.execute_command("GETSET", key, value)

    def mget(self, keys, *args):
        """
        Multi-get, return the strings values of the keys
        """
        keys = list_or_args("mget", keys, args)
        return self.execute_command("MGET", *keys)

    def setnx(self, key, value):
        """
        Set a key to a string value if the key does not exist
        """
        return self.execute_command("SETNX", key, value)

    def setex(self, key, time, value):
        """
        Set+Expire combo command
        """
        return self.execute_command("SETEX", key, time, value)

    def mset(self, mapping):
        """
        Set the respective fields to the respective values.
        HMSET replaces old values with new values.
        """
        items = []
        for pair in mapping.iteritems():
            items.extend(pair)
        return self.execute_command("MSET", *items)

    def msetnx(self, mapping):
        """
        Set multiple keys to multiple values in a single atomic
        operation if none of the keys already exist
        """
        items = []
        for pair in mapping.iteritems():
            items.extend(pair)
        return self.execute_command("MSETNX", *items)

    def incr(self, key, amount=1):
        """
        Increment the integer value of key
        """
        return self.execute_command("INCRBY", key, amount)

    def incrby(self, key, amount):
        """
        Increment the integer value of key by integer
        """
        return self.incr(key, amount)

    def decr(self, key, amount=1):
        """
        Decrement the integer value of key
        """
        return self.execute_command("DECRBY", key, amount)

    def decrby(self, key, amount):
        """
        Decrement the integer value of key by integer
        """
        return self.decr(key, amount)

    def append(self, key, value):
        """
        Append the specified string to the string stored at key
        """
        return self.execute_command("APPEND", key, value)

    def substr(self, key, start, end=-1):
        """
        Return a substring of a larger string
        """
        return self.execute_command("SUBSTR", key, start, end)

    # Commands operating on lists
    def push(self, key, value, tail=False):
        warnings.warn(DeprecationWarning(
            "redis.push() has been deprecated, "
            "use redis.lpush() or redis.rpush() instead"))

        return tail and self.rpush(key, value) or self.lpush(key, value)

    def rpush(self, key, value):
        """
        Append an element to the tail of the List value at key
        """
        return self.execute_command("RPUSH", key, value)

    def lpush(self, key, value):
        """
        Append an element to the head of the List value at key
        """
        return self.execute_command("LPUSH", key, value)

    def llen(self, key):
        """
        Return the length of the List value at key
        """
        return self.execute_command("LLEN", key)

    def lrange(self, key, start, end):
        """
        Return a range of elements from the List at key
        """
        return self.execute_command("LRANGE", key, start, end)

    def ltrim(self, key, start, end):
        """
        Trim the list at key to the specified range of elements
        """
        return self.execute_command("LTRIM", key, start, end)

    def lindex(self, key, index):
        """
        Return the element at index position from the List at key
        """
        return self.execute_command("LINDEX", key, index)

    def lset(self, key, index, value):
        """
        Set a new value as the element at index position of the List at key
        """
        return self.execute_command("LSET", key, index, value)

    def lrem(self, key, count, value):
        """
        Remove the first-N, last-N, or all the elements matching value
        from the List at key
        """
        return self.execute_command("LREM", key, count, value)

    def pop(self, key, tail=False):
        warnings.warn(DeprecationWarning(
            "redis.pop() has been deprecated, "
            "user redis.lpop() or redis.rpop() instead"))

        return tail and self.rpop(key) or self.lpop(key)

    def lpop(self, key):
        """
        Return and remove (atomically) the first element of the List at key
        """
        return self.execute_command("LPOP", key)

    def rpop(self, key):
        """
        Return and remove (atomically) the last element of the List at key
        """
        return self.execute_command("RPOP", key)

    def blpop(self, keys, timeout=0):
        """
        Blocking LPOP
        """
        if isinstance(keys, (str, unicode)):
            keys = [keys]
        else:
            keys = list(keys)

        keys.append(timeout)
        return self.execute_command("BLPOP", *keys)

    def brpop(self, keys, timeout=0):
        """
        Blocking RPOP
        """
        if isinstance(keys, (str, unicode)):
            keys = [keys]
        else:
            keys = list(keys)

        keys.append(timeout)
        return self.execute_command("BRPOP", *keys)

    def rpoplpush(self, srckey, dstkey):
        """
        Return and remove (atomically) the last element of the source
        List  stored at srckey and push the same element to the
        destination List stored at dstkey
        """
        return self.execute_command("RPOPLPUSH", srckey, dstkey)

    # Commands operating on sets
    def sadd(self, key, member):
        """
        Add the specified member to the Set value at key
        """
        return self.execute_command("SADD", key, member)

    def srem(self, key, member):
        """
        Remove the specified member from the Set value at key
        """
        return self.execute_command("SREM", key, member)

    def spop(self, key):
        """
        Remove and return (pop) a random element from the Set value at key
        """
        return self.execute_command("SPOP", key)

    def smove(self, srckey, dstkey, member):
        """
        Move the specified member from one Set to another atomically
        """
        return self.execute_command("SMOVE", srckey, dstkey, member)

    def scard(self, key):
        """
        Return the number of elements (the cardinality) of the Set at key
        """
        return self.execute_command("SCARD", key)

    def sismember(self, key, value):
        """
        Test if the specified value is a member of the Set at key
        """
        return self.execute_command("SISMEMBER", key, value)

    def sinter(self, keys, *args):
        """
        Return the intersection between the Sets stored at key1, key2, ..., keyN
        """
        keys = list_or_args("sinter", keys, args)
        return self.execute_command("SINTER", *keys)

    def sinterstore(self, dstkey, keys, *args):
        """
        Compute the intersection between the Sets stored
        at key1, key2, ..., keyN, and store the resulting Set at dstkey
        """
        keys = list_or_args("sinterstore", keys, args)
        return self.execute_command("SINTERSTORE", dstkey, *keys)

    def sunion(self, keys, *args):
        """
        Return the union between the Sets stored at key1, key2, ..., keyN
        """
        keys = list_or_args("sunion", keys, args)
        return self.execute_command("SUNION", *keys)

    def sunionstore(self, dstkey, keys, *args):
        """
        Compute the union between the Sets stored
        at key1, key2, ..., keyN, and store the resulting Set at dstkey
        """
        keys = list_or_args("sunionstore", keys, args)
        return self.execute_command("SUNIONSTORE", dstkey, *keys)

    def sdiff(self, keys, *args):
        """
        Return the difference between the Set stored at key1 and
        all the Sets key2, ..., keyN
        """
        keys = list_or_args("sdiff", keys, args)
        return self.execute_command("SDIFF", *keys)

    def sdiffstore(self, dstkey, keys, *args):
        """
        Compute the difference between the Set key1 and all the
        Sets key2, ..., keyN, and store the resulting Set at dstkey
        """
        keys = list_or_args("sdiffstore", keys, args)
        return self.execute_command("SDIFFSTORE", dstkey, *keys)

    def smembers(self, key):
        """
        Return all the members of the Set value at key
        """
        return self.execute_command("SMEMBERS", key)

    def srandmember(self, key):
        """
        Return a random member of the Set value at key
        """
        return self.execute_command("SRANDMEMBER", key)

    # Commands operating on sorted zsets (sorted sets)
    def zadd(self, key, score, member):
        """
        Add the specified member to the Sorted Set value at key
        or update the score if it already exist
        """
        return self.execute_command("ZADD", key, score, member)

    def zrem(self, key, member):
        """
        Remove the specified member from the Sorted Set value at key
        """
        return self.execute_command("ZREM", key, member)

    def zincr(self, key, member):
        return self.zincrby(key, 1, member)

    def zdecr(self, key, member):
        return self.zincrby(key, -1, member)

    def zincrby(self, key, increment, member):
        """
        If the member already exists increment its score by increment,
        otherwise add the member setting increment as score
        """
        return self.execute_command("ZINCRBY", key, increment, member)

    def zrank(self, key, member):
        """
        Return the rank (or index) or member in the sorted set at key,
        with scores being ordered from low to high
        """
        return self.execute_command("ZRANK", key, member)

    def zrevrank(self, key, member):
        """
        Return the rank (or index) or member in the sorted set at key,
        with scores being ordered from high to low
        """
        return self.execute_command("ZREVRANK", key, member)

    def zrange(self, key, start, end):
        """
        Return a range of elements from the sorted set at key

        """
        return self.execute_command("ZRANGE", key, start, end)

    def zrevrange(self, key, start, end):
        """
        Return a range of elements from the sorted set at key,
        exactly like ZRANGE, but the sorted set is ordered in
        traversed in reverse order, from the greatest to the smallest score
        """
        return self.execute_command("ZREVRANGE", key, start, end)

    def zrangebyscore(self, key, min, max):
        """
        Return all the elements with score >= min and score <= max
        (a range query) from the sorted set
        """
        return self.execute_command("ZRANGEBYSCORE", key, min, max)

    def zcount(self, key, min, max):
        """
        Return the number of elements with score >= min and score <= max
        in the sorted set
        """
        return self.execute_command("ZCOUNT", key, min, max)

    def zcard(self, key):
        """
        Return the cardinality (number of elements) of the sorted set at key
        """
        return self.execute_command("ZCARD", key)

    def zscore(self, key, element):
        """
        Return the score associated with the specified element of the sorted
        set at key
        """
        return self.execute_command("ZSCORE", key, element)

    def zremrangebyrank(self, key, min, max):
        """
        Remove all the elements with rank >= min and rank <= max from
        the sorted set
        """
        return self.execute_command("ZREMRANGEBYRANK", key, min, max)

    def zremrangebyscore(self, key, min, max):
        """
        Remove all the elements with score >= min and score <= max from
        the sorted set
        """
        return self.execute_command("ZREMRANGEBYSCORE", key, min, max)

    def zunionstore(self, dstkey, keys, aggregate=None):
        """
        Perform a union over a number of sorted sets with optional
        weight and aggregate
        """
        return self._zaggregate("ZUNIONSTORE", dstkey, keys, aggregate)

    def zinterstore(self, dstkey, keys, aggregate=None):
        """
        Perform an intersection over a number of sorted sets with optional
        weight and aggregate
        """
        return self._zaggregate("ZINTERSTORE", dstkey, keys, aggregate)

    def _zaggregate(self, command, dstkey, keys, aggregate):
        pieces = [command, dstkey, len(keys)]
        if isinstance(keys, dict):
            items = keys.items()
            keys = [i[0] for i in items]
            weights = [i[1] for i in items]
        else:
            weights = None

        pieces.extend(keys)
        if weights:
            pieces.append("WEIGHTS")
            pieces.extend(weights)
        if aggregate:
            pieces.append("AGGREGATE")
            pieces.append(aggregate)

        return self.execute_command(*pieces)

    # Commands operating on hashes
    def hset(self, key, field, value):
        """
        Set the hash field to the specified value. Creates the hash if needed
        """
        return self.execute_command("HSET", key, field, value)

    def hsetnx(self, key, field, value):
        """
        Set the hash field to the specified value if the field does not exist.
        Creates the hash if needed
        """
        return self.execute_command("HSETNX", key, field, value)

    def hget(self, key, field):
        """
        Retrieve the value of the specified hash field.
        """
        return self.execute_command("HGET", key, field)

    def hmget(self, key, fields):
        """
        Get the hash values associated to the specified fields.
        """
        return self.execute_command("HMGET", key, *fields)

    def hmset(self, key, mapping):
        """
        Set the hash fields to their respective values.
        """
        items = []
        for pair in mapping.iteritems():
            items.extend(pair)
        return self.execute_command("HMSET", key, *items)

    def hincr(self, key, field):
        return self.hincrby(key, field, 1)

    def hdecr(self, key, field):
        return self.hincrby(key, field, -1)

    def hincrby(self, key, field, integer):
        """
        Increment the integer value of the hash at key on field with integer.
        """
        return self.execute_command("HINCRBY", key, field, integer)

    def hexists(self, key, field):
        """
        Test for existence of a specified field in a hash
        """
        return self.execute_command("HEXISTS", key, field)

    def hdel(self, key, field):
        """
        Remove the specified field from a hash
        """
        return self.execute_command("HDEL", key, field)

    def hlen(self, key):
        """
        Return the number of items in a hash.
        """
        return self.execute_command("HLEN", key)

    def hkeys(self, key):
        """
        Return all the fields in a hash.
        """
        return self.execute_command("HKEYS", key)

    def hvals(self, key):
        """
        Return all the values in a hash.
        """
        return self.execute_command("HVALS", key)

    @defer.inlineCallbacks
    def hgetall(self, key):
        """
        Return all the fields and associated values in a hash.
        """
        d = yield self.execute_command("HGETALL", key)
        defer.returnValue(dict(zip(d[::2], d[1::2])))

    # Sorting
    def sort(self, key, start=None, end=None, by=None, get=None,
            desc=None, alpha=False, store=None):
        if (start is not None and end is not None) or \
            (end is not None and start is None):
            raise RedisError("``start`` and ``end`` must both be specified")

        pieces = [key]
        if by is not None:
            pieces.append("BY")
            pieces.append(by)
        if start is not None and end is not None:
            pieces.append("LIMIT")
            pieces.append(start)
            pieces.append(end)
        if get is not None:
            pieces.append("GET")
            pieces.append(get)
        if desc:
            pieces.append("DESC")
        if alpha:
            pieces.append("ALPHA")
        if store is not None:
            pieces.append("STORE")
            pieces.append(store)

        return self.execute_command("SORT", *pieces)

    # Transactions
    # multi() will return a deferred with a "connection" object
    # That object must be used for further interactions within
    # the transaction. At the end, either exec() or discard()
    # must be executed.
    @defer.inlineCallbacks
    def multi(self):
        response = yield self.execute_command("MULTI")
        if response == "OK":
            self.inTransaction = True
        defer.returnValue(self)

    def commit(self):
        if self.inTransaction is False:
            raise RedisError("Not in transaction")
        return self.execute_command("EXEC")

    def discard(self):
        if self.inTransaction is False:
            raise RedisError("Not in transaction")
        self.transactions = 0
        self.inTransaction = False
        return self.execute_command("DISCARD")

    # Publish/Subscribe
    # see the SubscriberProtocol for subscribing to channels
    def publish(self, channel, message):
        """
        Publish message to a channel
        """
        return self.execute_command("PUBLISH", channel, message)

    # Persistence control commands
    def save(self):
        """
        Synchronously save the DB on disk
        """
        return self.execute_command("SAVE")

    def bgsave(self):
        """
        Asynchronously save the DB on disk
        """
        return self.execute_command("BGSAVE")

    def lastsave(self):
        """
        Return the UNIX time stamp of the last successfully saving of the
        dataset on disk
        """
        return self.execute_command("LASTSAVE")

    def shutdown(self):
        """
        Synchronously save the DB on disk, then shutdown the server
        """
        self.factory.continueTrying = False
        return self.execute_command("SHUTDOWN")

    def bgrewriteaof(self):
        """
        Rewrite the append only file in background when it gets too big
        """
        return self.execute_command("BGREWRITEAOF")

    # Remote server control commands
    def info(self):
        """
        Provide information and statistics about the server
        """
        return self.execute_command("INFO")

    # monitor and slaveof should are missing


class SubscriberProtocol(RedisProtocol):
    def connectionLost(self, why):
        pass

    def messageReceived(self, pattern, channel, message):
        pass

    def replyReceived(self, reply):
        if isinstance(reply, list):
            if reply[-3] == u"message":
                self.messageReceived(None, *reply[-2:])
            else:
                self.messageReceived(*reply[-3:])

    def subscribe(self, channels):
        if isinstance(channels, (str, unicode)):
            channels = [channels]
        return self.execute_command("SUBSCRIBE", *channels)

    def unsubscribe(self, channels):
        if isinstance(channels, (str, unicode)):
            channels = [channels]
        return self.execute_command("UNSUBSCRIBE", *channels)

    def psubscribe(self, patterns):
        if isinstance(patterns, (str, unicode)):
            patterns = [patterns]
        return self.execute_command("PSUBSCRIBE", *patterns)

    def punsubscribe(self, patterns):
        if isinstance(patterns, (str, unicode)):
            patterns = [patterns]
        return self.execute_command("PUNSUBSCRIBE", *patterns)

class SubscriberFactory(protocol.ReconnectingClientFactory):
    maxDelay = 120
    continueTrying = True
    protocol = SubscriberProtocol


class ConnectionHandler(object):
    def __init__(self, factory):
        self._factory = factory
        self._connected = factory.deferred

    def _wait_pool_cleanup(self, deferred):
        if self._factory.size == 0:
            deferred.callback(True)

    def disconnect(self):
        self._factory.continueTrying = 0
        for conn in self._factory.pool:
            try:
                conn.transport.loseConnection()
            except:
                pass

        d = defer.Deferred()
        t = task.LoopingCall(self._wait_pool_cleanup, d)
        d.addCallback(lambda ign: t.stop())
        t.start(.5)
        return d

    def __getattr__(self, method):
        try:
            return getattr(self._factory.getConnection, method)
        except Exception, e:
            d = defer.Deferred()
            d.errback(e)
            return lambda *ign: d

    def __repr__(self):
        try:
            cli = self._factory.pool[0].transport.getPeer()
        except:
            return "<Redis Connection: Not connected>"
        else:
            return "<Redis Connection: %s:%s - %d connection(s)>" % \
                   (cli.host, cli.port, self._factory.size)


ShardedMethods = [
    "decr",
    "delete",
    "exists",
    "expire",
    "get",
    "get_type",
    "getset",
    "hdel",
    "hexists",
    "hget",
    "hgetall",
    "hincrby",
    "hkeys",
    "hlen",
    "hmget",
    "hmset",
    "hset",
    "hvals",
    "incr",
    "lindex",
    "llen",
    "lrange",
    "lrem",
    "lset",
    "ltrim",
    "pop",
    "publish",
    "push",
    "rename",
    "sadd",
    "set",
    "setex",
    "setnx",
    "sismember",
    "smembers",
    "srem",
    "ttl",
    "zadd",
    "zcard",
    "zincr",
    "zrange",
    "zrangebyscore",
    "zrem",
    "zremrangebyscore",
    "zrevrange",
    "zscore",
]

_findhash = re.compile('.+\{(.*)\}.*', re.I)

class HashRing(object):
    """Consistent hash for redis API"""
    def __init__(self, nodes=[], replicas=160):
        self.nodes=[]
        self.replicas=replicas
        self.ring={}
        self.sorted_keys=[]

        for n in nodes:
            self.add_node(n)

    def add_node(self, node):
        self.nodes.append(node)
        for x in xrange(self.replicas):
            crckey = zlib.crc32("%s:%d" % (node, x))
            self.ring[crckey] = node
            self.sorted_keys.append(crckey)

        self.sorted_keys.sort()

    def remove_node(self, node):
        self.nodes.remove(node)
        for x in xrange(self.replicas):
            crckey = zlib.crc32("%s:%d" % (node, x))
            self.ring.remove(crckey)
            self.sorted_keys.remove(crckey)

    def get_node(self, key):
        n, i = self.get_node_pos(key)
        return n
    #self.get_node_pos(key)[0]

    def get_node_pos(self, key):
        if len(self.ring) == 0:
            return [None, None]
        crc = zlib.crc32(key)
        idx = bisect.bisect(self.sorted_keys, crc)
        idx = min(idx, (self.replicas * len(self.nodes))-1) # prevents out of range index
        return [self.ring[self.sorted_keys[idx]], idx]

    def iter_nodes(self, key):
        if len(self.ring) == 0: yield None, None
        node, pos = self.get_node_pos(key)
        for k in self.sorted_keys[pos:]:
            yield k, self.ring[k]

    def __call__(self, key):
        return self.get_node(key)

class ShardedConnectionHandler(object):
    def __init__(self, connections):
        if isinstance(connections, defer.DeferredList):
            self._ring = None
            connections.addCallback(self._makeRing)
        else:
            self._ring = HashRing(connections)

    def _makeRing(self, connections):
        connections = map(operator.itemgetter(1), connections)
        self._ring = HashRing(connections)
        return self

    @defer.inlineCallbacks
    def disconnect(self):
        if not self._ring:
            raise ConnectionError("Not connected")

        for conn in self._ring.nodes:
            yield conn.disconnect()
        defer.returnValue(True)

    def _wrap(self, method, *args, **kwargs):
        try:
            key = args[0]
            assert isinstance(key, (str, unicode))
        except:
            raise ValueError(
                  "Method '%s' requires a key as the first argument" % method)

        m = _findhash.match(key)
        if m is not None and len(m.groups()) >= 1:
            node = self._ring(m.groups()[0])
        else:
            node = self._ring(key)

        return getattr(node, method)(*args, **kwargs)

    def __getattr__(self, method):
        if method in ShardedMethods:
            return functools.partial(self._wrap, method)
        else:
            raise NotImplementedError("Method '%s' cannot be sharded" % method)

    @defer.inlineCallbacks
    def mget(self, keys, *args):
        """
        high-level mget, required because of the sharding support
        """

        keys = list_or_args("mget", keys, args)
        group = collections.defaultdict(lambda: [])
        for k in keys:
            node = self._ring(k)
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
        for conn in self._ring.nodes:
            try:
                cli = conn._factory.pool[0].transport.getPeer()
            except:
                pass
            else:
                nodes.append("%s:%s/%d" % \
                             (cli.host, cli.port, conn._factory.size))
        return "<Redis Sharded Connection: %s>" % ", ".join(nodes)


class RedisFactory(protocol.ReconnectingClientFactory):
    maxDelay = 10
    protocol = RedisProtocol

    def __init__(self, dbid, poolsize, isLazy=False):
        if not isinstance(poolsize, int):
            raise ValueError("Redis poolsize must be an integer, not %s" % \
                             repr(poolsize))

        if not isinstance(dbid, int):
            raise ValueError("Redis dbid must be an integer, not %s" % \
                             repr(dbid))

        self.dbid = dbid
        self.poolsize = poolsize
        self.isLazy = isLazy

        self.idx = 0
        self.size = 0
        self.pool = []
        self.deferred = defer.Deferred()
        self.handler = ConnectionHandler(self)

    def addConnection(self, conn):
        self.pool.append(conn)
        self.size = len(self.pool)
        if self.deferred:
            if self.size == self.poolsize:
                self.deferred.callback(self.handler)
                self.deferred = None

    def delConnection(self, conn):
        try:
            self.pool.remove(conn)
        except Exception, e:
            log.msg("Could not remove connection from pool: %s" % str(e))

        self.size = len(self.pool)

    def connectionError(self, why):
        if self.deferred:
            self.deferred.errback(ValueError(why))
            self.deferred = None

    @property
    def getConnection(self):
        if not self.size:
            raise ConnectionError("Not connected")

        n = self.size
        while n:
            conn = self.pool[self.idx % self.size]
            self.idx += 1
            if conn.inTransaction is False:
                return conn
            n -= 1

        raise RedisError("In transaction")


def makeConnection(host, port, dbid, poolsize, reconnect, isLazy):
    factory = RedisFactory(dbid, poolsize, isLazy)
    factory.continueTrying = reconnect
    for x in xrange(poolsize):
        reactor.connectTCP(host, port, factory)

    if isLazy:
        return factory.handler
    else:
        return factory.deferred

def makeShardedConnection(hosts, dbid, poolsize, reconnect, isLazy):
    err = "Please use a list or tuple of host:port for sharded connections"
    if not isinstance(hosts, (list, tuple)):
        raise ValueError(err)

    connections = []
    for item in hosts:
        try:
            host, port = item.split(":")
            port = int(port)
        except:
            raise ValueError(err)

    c = makeConnection(host, port, dbid, poolsize, reconnect, isLazy)
    connections.append(c)

    if isLazy:
        return ShardedConnectionHandler(connections)
    else:
        deferred = defer.DeferredList(connections)
        ShardedConnectionHandler(deferred)
        return deferred

def Connection(host="localhost", port=6379, dbid=0, reconnect=True):
    return makeConnection(host, port, dbid, 1, reconnect, False)

def lazyConnection(host="localhost", port=6379, dbid=0, reconnect=True):
    return makeConnection(host, port, dbid, 1, reconnect, True)

def ConnectionPool(host="localhost", port=6379, dbid=0,
                   poolsize=10, reconnect=True):
    return makeConnection(host, port, dbid, poolsize, reconnect, False)

def lazyConnectionPool(host="localhost", port=6379, dbid=0,
                       poolsize=10, reconnect=True):
    return makeConnection(host, port, dbid, poolsize, reconnect, True)

def ShardedConnection(hosts, dbid=0, reconnect=True):
    return makeShardedConnection(hosts, dbid, 1, reconnect, False)

def lazyShardedConnection(hosts, dbid=0, reconnect=True):
    return makeShardedConnection(hosts, dbid, 1, reconnect, True)

def ShardedConnectionPool(hosts, dbid=0, poolsize=10, reconnect=True):
    return makeShardedConnection(hosts, dbid, poolsize, reconnect, False)

def lazyShardedConnectionPool(hosts, dbid=0, poolsize=10, reconnect=True):
    return makeShardedConnection(hosts, dbid, poolsize, reconnect, True)

__all__ = [
    Connection, lazyConnection,
    ConnectionPool, lazyConnectionPool,
    ShardedConnection, lazyShardedConnection,
    ShardedConnectionPool, lazyShardedConnectionPool,
]

__version__ = version = "0.3"
__author__ = "Alexandre Fiori"
