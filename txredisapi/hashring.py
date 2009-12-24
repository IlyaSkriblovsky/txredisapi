#
# consistent hashing for redis client
# based on ezmobius client (redis-rb)
#
import zlib
import bisect

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
        return self.get_node_pos(key)[0]

    def get_node_pos(self, key):
        if len(self.ring) == 0:
            return [None, None]
        crc = zlib.crc32(key)
        idx = bisect.bisect(self.sorted_keys, crc)
        return [self.ring[self.sorted_keys[idx]], idx]

    def iter_nodes(self, key):
        if len(self.ring) == 0:
            return [None, None]
        node, pos = self.get_node_pos(key)
        for k in self.sorted_keys[pos:]:
            #yield self.ring[k]
            pass        

    def __call__(self, key):
        return self.get_node(key)
   

if __name__ == "__main__":
    ch = HashRing(["teste","teste2","teste3"])
    print ch.get_node("alexandre")
    print ch.get_node("gleicon")
    print ch.get_node("asdasjkdjkaskjdaskjdaskjgleicon")
    print ch.get_node("aiaiaiaia_asdasjkdjkaskjdaskjdaskjgleicon")
