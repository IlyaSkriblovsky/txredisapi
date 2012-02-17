#!/usr/bin/env twistd -ny
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
# run:
#  twistd -ny twistwedweb_server.py

import txredisapi as redis

from twisted.application import internet
from twisted.application import service
from twisted.internet import defer
from twisted.web import server
from twisted.web import xmlrpc
from twisted.web.resource import Resource

class Root(Resource):
    isLeaf = False

class BaseHandler(object):
    isLeaf = True
    def __init__(self, db):
        self.db = db
        Resource.__init__(self)

class IndexHandler(BaseHandler, Resource):
    def _success(self, value, request, message):
        request.write(message % repr(value))
        request.finish()

    def _failure(self, error, request, message):
        request.write(message % str(error))
        request.finish()

    def render_GET(self, request):
        try:
            key = request.args["key"][0]
        except:
            request.setResponseCode(404, "not found")
            return ""

        d = self.db.get(key)
        d.addCallback(self._success, request, "get: %s\n")
        d.addErrback(self._failure, request, "get failed: %s\n")
        return server.NOT_DONE_YET

    def render_POST(self, request):
        try:
            key = request.args["key"][0]
            value = request.args["value"][0]
        except:
            request.setResponseCode(404, "not found")
            return ""

        d = self.db.set(key, value)
        d.addCallback(self._success, request, "set: %s\n")
        d.addErrback(self._failure, request, "set failed: %s\n")
        return server.NOT_DONE_YET


class InfoHandler(BaseHandler, Resource):
    def render_GET(self, request):
        return "redis: %s\n" % repr(self.db)


class XmlrpcHandler(BaseHandler, xmlrpc.XMLRPC):
    allowNone = True

    @defer.inlineCallbacks
    def xmlrpc_get(self, key):
        value = yield self.db.get(key)
        defer.returnValue(value)

    @defer.inlineCallbacks
    def xmlrpc_set(self, key, value):
        result = yield self.db.set(key, value)
        defer.returnValue(result)


# redis connection
_db = redis.lazyConnectionPool()

# http resources
root = Root()
root.putChild("", IndexHandler(_db))
root.putChild("info", InfoHandler(_db))
root.putChild("xmlrpc", XmlrpcHandler(_db))

application = service.Application("webredis")
srv = internet.TCPServer(8888, server.Site(root), interface="127.0.0.1")
srv.setServiceParent(application)
