#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function

import txredisapi as redis

from twisted.internet import defer
from twisted.internet import reactor


@defer.inlineCallbacks
def transactions():
    """
    The multi() method on txredisapi connections returns a transaction.
    All redis commands called on transactions are then queued by the server.
    Calling the transaction's .commit() method executes all the queued commands
    and returns the result for each queued command in a list.
    Calling the transaction's .discard() method flushes the queue and returns
    the connection to the default non-transactional state.

    multi() also takes an optional argument keys, which can be either a
    string or an iterable (list,tuple etc) of strings. If present, the keys
    are WATCHED on the server, and if any of the keys are modified by
    a different connection before the transaction is committed,
    commit() raises a WatchError exception.

    Transactions with WATCH make multi-command atomic all or nothing operations
    possible. If a transaction fails, you can be sure that none of the commands
    in it ran and you can retry it again.
    Read the redis documentation on Transactions for more.
    http://redis.io/topics/transactions

    Tip: Try to keep transactions as short as possible.
    Connections in a transaction cannot be reused until the transaction
    is either committed or discarded. For instance, if you have a
    ConnectionPool with 10 connections and all of them are in transactions,
    if you try to run a command on the connection pool,
    it'll raise a RedisError exception.
    """
    conn = yield redis.Connection()
    # A Transaction with nothing to watch on
    txn = yield conn.multi()
    txn.incr('test:a')
    txn.lpush('test:l', 'test')
    r = yield txn.commit()  # Commit txn,call txn.discard() to discard it
    print('Transaction1: %s' % r)

    # Another transaction with a few values to watch on
    txn1 = yield conn.multi(['test:l', 'test:h'])
    txn1.lpush('test:l', 'test')
    txn1.hset('test:h', 'test', 'true')
    # Transactions with watched keys will fail if any of the keys are modified
    # externally after calling .multi() and before calling transaction.commit()
    r = yield txn1.commit()  # This will raise if WatchError if txn1 fails.
    print('Transaction2: %s' % r)
    yield conn.disconnect()


def main():
    transactions().addCallback(lambda ign: reactor.stop())

if __name__ == '__main__':
    reactor.callWhenRunning(main)
    reactor.run()
