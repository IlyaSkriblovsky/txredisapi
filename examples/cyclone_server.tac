#!/usr/bin/env python
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
# requires cyclone:
#  http://github.com/fiorix/tornado
# run:
#  twistd -ny cyclone_server.tac

import txredisapi
import cyclone.web
from twisted.internet import defer
from twisted.application import service, internet

class IndexHandler(cyclone.web.RequestHandler):
    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def get(self):
        key = self.get_argument("key")
        try:
            value = yield self.settings.db.get(key.encode("utf-8"))
        except Exception, e:
            self.write("get failed: %s\n" % str(e))
        else:
            self.write("get: %s\n" % repr(value))
        self.finish()

    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def post(self):
        key = self.get_argument("key")
        value = self.get_argument("value")
        try:
            result = yield self.settings.db.set(key.encode("utf-8"), value.encode("utf-8"))
        except Exception, e:
            self.write("set failed: %s\n" % str(e))
        else:
            self.write("set: %s\n" % result)
        self.finish()


class InfoHandler(cyclone.web.RequestHandler):
    def get(self):
        self.write("redis: %s\n" % repr(self.settings.db))
        #self.settings.db.disconnect()


class XmlrpcHandler(cyclone.web.XmlrpcRequestHandler):
    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def xmlrpc_get(self, key):
        value = yield self.settings.db.get(key)
        defer.returnValue(value)

    @defer.inlineCallbacks
    @cyclone.web.asynchronous
    def xmlrpc_set(self, key, value):
        result = yield self.settings.db.set(key, value)
        defer.returnValue(result)


class WebRedis(cyclone.web.Application):
    def __init__(self):
        handlers = [
            (r"/",       IndexHandler),
            (r"/info",   InfoHandler),
            (r"/xmlrpc", XmlrpcHandler),
        ]

        settings = {
            "db": txredisapi.lazyRedisConnectionPool(),
            #"static_path": "./static",
        }

        cyclone.web.Application.__init__(self, handlers, **settings)


application = service.Application("webredis")
srv = internet.TCPServer(8888, WebRedis(), interface="127.0.0.1")
srv.setServiceParent(application)
