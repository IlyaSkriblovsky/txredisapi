#!/usr/bin/env python
# coding: utf-8

import sys
import txredisapi as redis

from twisted.internet import defer
from twisted.internet import reactor

def wordfreq(file):
    try:
        f = open(file, 'r')
        words = f.read()
        f.close()
    except Exception, e:
        print "Exception: %s" % e
        return None

    wf={}
    wlist = words.split()
    for b in wlist:
        a=b.lower()
        if wf.has_key(a):
            wf[a]=wf[a]+1
        else:
           wf[a]=1
    return len(wf), wf


@defer.inlineCallbacks
def main(wordlist):
    db = yield redis.ShardedConnection(("localhost:6379", "localhost:6380"))
    for k in wordlist:
        yield db.set(k, 1)

    reactor.stop()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: wordfreq.py <file_to_count.txt>"
        sys.exit(-1)
    l, wfl = wordfreq(sys.argv[1])
    print "count: %d" % l
    main(wfl.keys())
    reactor.run()
