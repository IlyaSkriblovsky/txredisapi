#!/usr/bin/env python
# coding: utf-8

from txredisapi import HashRing
from collections import defaultdict

if __name__ == "__main__":
    ch = HashRing(["server1", "server2", "server3"])
    key_history={}
    node_histogram=defaultdict(lambda: 0)
    for x in xrange(1000):
        key_history[x]=ch.get_node("k:%d" % x)
        s=key_history[x]
        node_histogram[s]=node_histogram[s]+1
    print "server\t\tkeys:"
    for a in node_histogram.keys():
        print "%s:\t%d" % (a, node_histogram[a])
