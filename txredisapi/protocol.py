""" 
@file protocol.py
@author Dorian Raymer
@author Ludovico Magnocavallo
@date 9/30/09
@brief Twisted compatible version of redis.py

@mainpage

txRedis is an asynchronous, Twisted, version of redis.py (included in the
redis server source).

The official Redis Command Reference:
http://code.google.com/p/redis/wiki/CommandReference

@section An example demonstrating how to use the client in your code:
@code
from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet import defer

from txredis.protocol import Redis

@defer.inlineCallbacks
def main():
    clientCreator = protocol.ClientCreator(reactor, Redis)
    redis = yield clientCreator.connectTCP(HOST, PORT)
    
    res = yield redis.ping()
    print res

    res = yield redis.set('test', 42)
    print res
    
    test = yield redis.get('test')
    print res

@endcode

Redis google code project: http://code.google.com/p/redis/
"""


import types
from twisted.python import log
from twisted.internet import defer
from twisted.protocols import basic
from twisted.protocols import policies


class RedisError(Exception): pass
class ConnectionError(RedisError): pass
class ResponseError(RedisError): pass
class InvalidResponse(RedisError): pass
class InvalidData(RedisError): pass


class RedisProtocol(basic.LineReceiver, policies.TimeoutMixin):
    """The main Redis client.
    """

    ERROR = "-"
    STATUS = "+"
    INTEGER = ":"
    BULK = "$"
    MULTI_BULK = "*"

    def __init__(self, charset='utf8', errors='strict'):
        self.charset = charset
        self.errors = errors

        self.bulk_length = 0
        self.bulk_buffer = ""
        self.multi_bulk_length = 0
        self.multi_bulk_reply = []
        self.replyQueue = defer.DeferredQueue()
        
    @defer.inlineCallbacks
    def connectionMade(self):
        if self.factory.db:
            try:
                r = yield self.select(self.factory.db)
                if isinstance(r, ResponseError):
                    raise r
            except Exception, e:
                self.factory.continueTrying = False
                self.transport.loseConnection()
                msg = "REDIS ERROR: Could not set DB=%s: %s" % (self.factory.db, e)
                self.factory.error(msg)
                if self.factory.isLazy:
                    log.msg(msg)
                defer.returnValue(None)

        self.connected = 1
        self.factory.append(self)

    def connectionLost(self, reason):
        self.connected = 0
        self.factory.remove(self)
        basic.LineReceiver.connectionLost(self, reason)

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
                self.replyReceived(InvalidResponse("Cannot convert data '%s' to integer" % data))
                return 
            if self.bulk_length == -1:
                self.bulkDataReceived(None)
                return
            else:
                self.setRawMode()
        elif token == self.MULTI_BULK:
            try:
                self.multi_bulk_length = long(data)
            except (TypeError, ValueError):
                self.replyReceived(InvalidResponse("Cannot convert multi-response header '%s' to integer" % data))
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
            buffer = self.bulk_buffer
            self.bulk_buffer = ""
            self.bulkDataReceived(buffer)
            self.setLineMode(extra=rest)

    def errorReceived(self, data):
        """
        Error from server.
        """
        reply = ResponseError(data[4:] if data[:4] == 'ERR ' else data)
        self.replyReceived(reply)

    def statusReceived(self, data):
        """
        Single line status should always be a string.
        """
        if data == 'none':
            reply = None # should this happen here in the client?
        else:
            reply = data 
        self.replyReceived(reply)

    def integerReceived(self, data):
        """
        For handling integer replies.
        """
        try:
            reply = int(data) 
        except ValueError:
            reply = InvalidResponse("Cannot convert data '%s' to integer" % data)
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
                element = data.decode(self.charset)

        if self.multi_bulk_length > 0:
            self.handleMultiBulkElement(element)
        else:
            self.replyReceived(element)

    def handleMultiBulkElement(self, element):
        self.multi_bulk_reply.append(element)
        self.multi_bulk_length = self.multi_bulk_length - 1
        if self.multi_bulk_length == 0:
            self.multiBulkDataReceived()


    def multiBulkDataReceived(self):
        """
        Receipt of list or set of bulk data elements.
        """
        reply = self.multi_bulk_reply
        self.multi_bulk_reply = []
        self.multi_bulk_length = 0
        self.replyReceived(reply)
        

    def replyReceived(self, reply):
        """
        Complete reply received and ready to be pushed to the requesting
        function.
        """
        self.replyQueue.put(reply)


    def get_response(self):
        """return deferred which will fire with response from server.
        """
        return self.replyQueue.get()

    def _encode(self, s):
        if isinstance(s, str):
            return s
        if isinstance(s, unicode):
            try:
                return s.encode(self.charset, self.errors)
            except UnicodeEncodeError, e:
                raise InvalidData("Error encoding unicode value '%s': %s" % (s.encode(self.charset, 'replace'), e))
        return str(s)
    
    def _write(self, s):
        """
        """
        self.transport.write(s)
            
    def ping(self):
        """
        Test command. Expect PONG as a reply.
        """
        self._write('PING\r\n')
        return self.get_response()
    
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
    # REDIS COMMANDS
    # 

    # Commands operating on string values
    def set(self, name, value, preserve=False, getset=False):
        """
        """
        # the following will raise an error for unicode values that can't be encoded to ascii
        # we could probably add an 'encoding' arg to init, but then what do we do with get()?
        # convert back to unicode? and what about ints, or pickled values?
        if getset: command = 'GETSET'
        elif preserve: command = 'SETNX'
        else: command = 'SET'
        value = self._encode(value)
        self._write('%s %s %s\r\n%s\r\n' % (
                command, name, len(value), value
            ))
        return self.get_response()
    
    def get(self, name):
        """
        """
        self._write('GET %s\r\n' % name)
        return self.get_response()
    
    def getset(self, name, value):
        """
        """
        return self.set(name, value, getset=True)
        
    def mget(self, *args):
        """
        """
        self._write('MGET %s\r\n' % ' '.join(args))
        return self.get_response()
    
    def incr(self, name, amount=1):
        """
        """
        if amount == 1:
            self._write('INCR %s\r\n' % name)
        else:
            self._write('INCRBY %s %s\r\n' % (name, amount))
        return self.get_response()

    def decr(self, name, amount=1):
        """
        """
        if amount == 1:
            self._write('DECR %s\r\n' % name)
        else:
            self._write('DECRBY %s %s\r\n' % (name, amount))
        return self.get_response()
    
    def exists(self, name):
        """
        """
        self._write('EXISTS %s\r\n' % name)
        return self.get_response()

    def delete(self, name):
        """
        """
        self._write('DEL %s\r\n' % name)
        return self.get_response()

    def get_type(self, name):
        """
        """
        self._write('TYPE %s\r\n' % name)
        res = self.get_response()
        # return None if res == 'none' else res
        return res
    
    # Commands operating on the key space
    @defer.inlineCallbacks
    def keys(self, pattern):
        """
        """
        self._write('KEYS %s\r\n' % pattern)
        # return self.get_response().split()
        r = yield self.get_response()
        if r is not None:
            res = r.split()
            res.sort()# XXX is sort ok?
        else:
            res = []
        defer.returnValue(res)
    
    def randomkey(self):
        """
        """
        #raise NotImplementedError("Implemented but buggy, do not use.")
        self._write('RANDOMKEY\r\n')
        return self.get_response()
    
    def rename(self, src, dst, preserve=False):
        """
        """
        if preserve:
            self._write('RENAMENX %s %s\r\n' % (src, dst))
            return self.get_response()
        else:
            self._write('RENAME %s %s\r\n' % (src, dst))
            return self.get_response() #.strip()
        
    def dbsize(self):
        """
        """
        self._write('DBSIZE\r\n')
        return self.get_response()
    
    def expire(self, name, time):
        """
        """
        self._write('EXPIRE %s %s\r\n' % (name, time))
        return self.get_response()
    
    def ttl(self, name):
        """
        """
        self._write('TTL %s\r\n' % name)
        return self.get_response()
    
    # Commands operating on lists
    def push(self, name, value, tail=False):
        """
        """
        value = self._encode(value)
        self._write('%s %s %s\r\n%s\r\n' % (
            'LPUSH' if tail else 'RPUSH', name, len(value), value
        ))
        return self.get_response()
    
    def llen(self, name):
        """
        """
        self._write('LLEN %s\r\n' % name)
        return self.get_response()

    def lrange(self, name, start, end):
        """
        """
        self._write('LRANGE %s %s %s\r\n' % (name, start, end))
        return self.get_response()
        
    def ltrim(self, name, start, end):
        """
        """
        self._write('LTRIM %s %s %s\r\n' % (name, start, end))
        return self.get_response()
    
    def lindex(self, name, index):
        """
        """
        self._write('LINDEX %s %s\r\n' % (name, index))
        return self.get_response()
        
    def pop(self, name, tail=False):
        """
        """
        self._write('%s %s\r\n' % ('RPOP' if tail else 'LPOP', name))
        return self.get_response()
    
    def lset(self, name, index, value):
        """
        """
        value = self._encode(value)
        self._write('LSET %s %s %s\r\n%s\r\n' % (
            name, index, len(value), value
        ))
        return self.get_response()
    
    def lrem(self, name, value, num=0):
        """
        """
        value = self._encode(value)
        self._write('LREM %s %s %s\r\n%s\r\n' % (
            name, num, len(value), value
        ))
        return self.get_response()
    
    # Commands operating on sets
    def sadd(self, name, value):
        """
        """
        value = self._encode(value)
        self._write('SADD %s %s\r\n%s\r\n' % (
            name, len(value), value
        ))
        return self.get_response()
        
    def srem(self, name, value):
        """
        """
        value = self._encode(value)
        self._write('SREM %s %s\r\n%s\r\n' % (
            name, len(value), value
        ))
        return self.get_response()
    
    def sismember(self, name, value):
        """
        """
        value = self._encode(value)
        self._write('SISMEMBER %s %s\r\n%s\r\n' % (
            name, len(value), value
        ))
        return self.get_response()
    
    @defer.inlineCallbacks
    def sinter(self, *args):
        """
        """
        self._write('SINTER %s\r\n' % ' '.join(args))
        res = yield self.get_response()
        if type(res) is list:
            res = set(res)
        defer.returnValue(res)
    
    def sinterstore(self, dest, *args):
        """
        """
        self._write('SINTERSTORE %s %s\r\n' % (dest, ' '.join(args)))
        return self.get_response()

    @defer.inlineCallbacks
    def smembers(self, name):
        """
        """
        self._write('SMEMBERS %s\r\n' % name)
        res = yield self.get_response()
        if type(res) is list:
            res = set(res)
        defer.returnValue(res)

    @defer.inlineCallbacks
    def sunion(self, *args):
        """
        """
        self._write('SUNION %s\r\n' % ' '.join(args))
        res = yield self.get_response()
        if type(res) is list:
            res = set(res)
        defer.returnValue(res)

    def sunionstore(self, dest, *args):
        """
        """
        self._write('SUNIONSTORE %s %s\r\n' % (dest, ' '.join(args)))
        return self.get_response()

    # Multiple databases handling commands
    def select(self, db):
        """
        """
        self._write('SELECT %s\r\n' % db)
        return self.get_response()
    
    def move(self, name, db):
        """
        """
        self._write('MOVE %s %s\r\n' % (name, db))
        return self.get_response()
    
    def flush(self, all_dbs=False):
        """
        """
        self._write('%s\r\n' % ('FLUSHALL' if all_dbs else 'FLUSHDB'))
        return self.get_response()
    
    # Persistence control commands
    def save(self, background=False):
        """
        """
        if background:
            self._write('BGSAVE\r\n')
        else:
            self._write('SAVE\r\n')
        return self.get_response()
        
    def lastsave(self):
        """
        """
        self._write('LASTSAVE\r\n')
        return self.get_response()
    
    @defer.inlineCallbacks
    def info(self):
        """
        """
        self._write('INFO\r\n')
        info = dict()
        res = yield self.get_response()
        res = res.split('\r\n')
        for l in res:
            if not l:
                continue
            k, v = l.split(':')
            info[k] = int(v) if v.isdigit() else v
        defer.returnValue(info)
    
    def sort(self, name, by=None, get=None, start=None, num=None, desc=False, alpha=False):
        """
        """
        stmt = ['SORT', name]
        if by:
            stmt.append("BY %s" % by)
        if start and num:
            stmt.append("LIMIT %s %s" % (start, num))
        if get is None:
            pass
        elif isinstance(get, basestring):
            stmt.append("GET %s" % get)
        elif isinstance(get, list) or isinstance(get, tuple):
            for g in get:
                stmt.append("GET %s" % g)
        else:
            raise RedisError("Invalid parameter 'get' for Redis sort")
        if desc:
            stmt.append("DESC")
        if alpha:
            stmt.append("ALPHA")
        self._write(' '.join(stmt + ["\r\n"]))
        return self.get_response()
    
    def auth(self, passwd):
        self._write('AUTH %s\r\n' % passwd)
        return self.get_response()
   
   # commands operating on sorted sets
    def zadd(self, name, score, member):
        """
        """
        value = self._encode(member)
        self._write('ZADD %s %s\r\n' % (
            name, score, value
        ))
        return self.get_response()
        
    def zrem(self, key, member):
        """
        """
        value = self._encode(value)
        self._write('ZREM %s %s\r\n' % (
            key, member
        ))
        return self.get_response()
    
    def zincr(self, key, member, incr=1):
        """
        """
        value = self._encode(value)
        self._write('ZINCRBY %s %s %s\r\n' % (
            key, incr, member
        ))
        return self.get_response()

    def zrange(self, key, start, end, withscores=False):
        """
        """
        if withscores:
            self._write('ZRANGE %s %s %s withscores\r\n' % (name, start, end))
        else:
            self._write('ZRANGE %s %s %s\r\n' % (name, start, end))

        return self.get_response()
   
    def zrevrange(self, key, start, end, withscores=False):
        """
        """
        if withscores:
            self._write('ZREVRANGE %s %s %s withscores\r\n' % (name, start, end))
        else:
            self._write('ZREVRANGE %s %s %s\r\n' % (name, start, end))

        return self.get_response()
    
    def zrangebyscore(self, key, min=0, max=2, withscores=False, limit=False, offset=0, count=1):
        """
            ZRANGEBYSCORE key min max [LIMIT offset count] (Redis >= 1.1)

            ZRANGEBYSCORE key min max [LIMIT offset count] [WITHSCORES] (Redis >= 1.3.4)

            Time complexity: O(log(N))+O(M) with N being the number of elements in the sorted set and M the number of elements returned by the command, so if M is constant (for instance you always ask for the first ten elements with LIMIT) you can consider it O(log(N))

            Return the all the elements in the sorted set at key with a score between min and max (including elements with score equal to min or max).
            The elements having the same score are returned sorted lexicographically as ASCII strings (this follows from a property of Redis sorted sets and does not involve further computation).
            Using the optional LIMIT it's possible to get only a range of the matching elements in an SQL-alike way. Note that if offset is large the commands needs to traverse the list for offset elements and this adds up to the O(M) figure.
            Return value

            Multi bulk reply, specifically a list of elements in the specified score range.
        """
        cmd = "ZRANGEBYSCORE %s %s %s" % (key, min, max)
        if limit:
                cmd=cmd + " LIMIT %s %s" % (offset, count)
        
        if withscores:
                cmd=cmd + " WITHSCORES"

        self._write(cmd)

        return self.get_response()

    def zremrangebyscore(self, key, minrange=0, maxrange=2):
        """
            ZREMRANGEBYSCORE key min max (Redis >= 1.1)

            Time complexity: O(log(N))+O(M) with N being the number of elements in the sorted set and M the number of elements removed by the operation

            Remove all the elements in the sorted set at key with a score between min and max (including elements with score equal to min or max).
            Return value

            Integer reply, specifically the number of elements removed.
        """
        cmd = "ZREMRANGEBYSCORE %s %s %s" % (key, minrange, maxrange)

        self._write(cmd)

        return self.get_response()
    
    def zcard(self, key):
        """
                ZCARD key (Redis >= 1.1)

                Time complexity O(1)

                Return the sorted set cardinality (number of elements). If the key does not exist 0 is returned, like for empty sorted sets.
                Return value

                Integer reply, specifically:

                the cardinality (number of elements) of the set as an integer.
        """
        value = self._encode(member)
        self._write('ZCARD %s\r\n' % (key))
        return self.get_response()
    
    def zscore(self, key, element):
        """
            ZSCORE key element (Redis >= 1.1)

            Time complexity O(1)

            Return the score of the specified element of the sorted set at key. If the specified element does not exist in the sorted set, or the key does not exist at all, a special 'nil' value is returned.
            Return value

            Bulk reply

            the score (a double precision floating point number) represented as string.
        """
        value = self._encode(member)
        self._write('ZSCORE %s %s\r\n' % (key, element))
        return self.get_response()
    
    # HASH functions

    def hset(self, key, hkey, hvalue, preserve=False):
        """
        Set the hash field to the specified value. Creates the hash if needed
        """
        if preserve: command = 'HSETNX'
        else: command = 'HSET'

        hvalue = self._encode(hvalue)
        self._write('%s %s %s %s\r\n%s\r\n' % (command, key, hkey, len(hvalue), hvalue))
        return self.get_response()
    
    def hget(self, key, hkey):
        """
        Retrieve the value of the specified hash field.
        """
        self._write('HGET %s %s\r\n%s\r\n' % (key, len(hkey), hkey))
        return self.get_response()
    
    def hdel(self, key, hkey):
        """
        Remove the specified field from a hash
        """
        self._write('HDEL %s %s\r\n%s\r\n' % (key, len(hkey), hkey))
        return self.get_response()
    
    def hincrby(self, key, hkey, amt=1):
        """
        Increment the integer value of the hash at key on field with integer (key, hash_key, ammount)
        """
        self._write('HINCRBY %s %s %s\r\n' % (key, hkey, amt))
        return self.get_response()
    
    def hlen(self, key):
        """
        Return the number of entries (fields) contained in the hash stored at key. If the specified key does not exist, 0 is returned assuming an empty hash.
        """
        self._write('HLEN %s\r\n' % (key))
        return self.get_response()
    
    def hkeys(self, key):
        """
        Return all keys in a hash.
        """
        self._write('HKEYS %s\r\n' % (key))
        return self.get_response()
    
    def hvals(self, key):
        """
        Return all values in a hash.
        """
        self._write('HVALS %s\r\n' % (key))
        return self.get_response()
    
    @defer.inlineCallbacks
    def hgetall(self, key):
        """
        Return all pairs of key/values in a hash.
        """
        self._write('HGETALL %s\r\n' % (key))
        result = {}
        items = yield self.get_response()
        for idx, k in enumerate(items):
            if not idx%2:
                result[k] = items[idx+1]
        defer.returnValue(result)

    def hexists(self, key, hkey):
        """
        Test for existence of a specified field in a hash
        """
        self._write('HEXISTS %s %s\r\n%s\r\n' % (key, len(hkey), hkey))
        return self.get_response()
    
    def hmget(self, key, arg_list):
        """
        Retrieve the values associated to the specified fields.
        """
        num_cmd = len(arg_list) + 2
        cmd = "*%s\r\n" % num_cmd
        cmd = cmd + "$5\r\nHMGET\r\n"
        cmd = cmd + "$%s\r\n%s\r\n" % (len(key), key)
        for m in arg_list:
            cmd = cmd + "$%s\r\n%s\r\n" % (len(m), m)

        self._write(cmd+"\r\n")
        return self.get_response()

    def hmset(self, key, kv_dict):
        """
        Set the respective fields to the respective values. HMSET replaces old values with new values.
        """
        num_cmd = (len(kv_dict) * 2) + 2
        cmd = "*%s\r\n" % num_cmd
        cmd = cmd + "$5\r\nHMSET\r\n"
        cmd = cmd + "$%s\r\n%s\r\n" % (len(key), key)
        
        listo = []
        [listo.extend(p) for p in kv_dict.iteritems()]
        for m in listo:
            mt = type(m)
            if mt in (types.IntType, types.FloatType):
                m = str(m)
            elif mt is types.UnicodeType:
                m = m.encode(self.charset)
            elif mt is not types.StringType:
                raise ValueError("Cannot store object of type %s: %s" % (mt, repr(m)))
            cmd = cmd + "$%s\r\n%s\r\n" % (len(m), m)
        self._write(cmd)
        return self.get_response()
    
    # PUB/SUB protocol

    def publish(self, channel, body):
        """
        Publish body to a channel 
        """
        self._write('*3\r\n$7\r\nPUBLISH\r\n$%s\r\n%s\r\n$%si\r\n%s\r\n' % (len(channel), channel, len(body), body))
        return self.get_response()


class SubscriberProtocol(RedisProtocol):
    def messageReceived(self, pattern, channel, message):
        pass

    def replyReceived(self, reply):
        if type(reply) is types.ListType:
            if reply[-3] == u"message":
                self.messageReceived(None, *reply[-2:])
            else:
                self.messageReceived(*reply[-3:])

    def __pubsub(self, command, channels):
        if type(channels) is types.StringType:
            channels = [channels]
        elif type(channels) is types.UnicodeType:
            channels = [channels.encode(self.charset, self.errors)]

        if type(channels) is not types.ListType:
            raise TypeError("channels must be either a string or a list of strings")

        for chan in channels:
            if type(chan) is types.UnicodeType:
                chan = chan.encode(self.charset)
            self._write('*2\r\n$%s\r\n%s\r\n$%s\r\n%s\r\n' % \
                (len(command), command, len(chan), chan))

    def subscribe(self, channels):
        self.__pubsub("SUBSCRIBE", channels)

    def unsubscribe(self, channels):
        self.__pubsub("UNSUBSCRIBE", channels)

    def psubscribe(self, patterns):
        self.__pubsub("PSUBSCRIBE", patterns)

    def punsubscribe(self, patterns):
        self.__pubsub("PUNSUBSCRIBE", patterns)
