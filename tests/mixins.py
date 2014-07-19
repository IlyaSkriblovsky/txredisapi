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

from twisted.trial import unittest
from twisted.internet import defer

REDIS_HOST = "localhost"
REDIS_PORT = 6379


class RedisVersionCheckMixin(object):
    @defer.inlineCallbacks
    def checkVersion(self, major, minor, patch=0):
        d = yield self.db.info("server")
        if u'redis_version' not in d:
            defer.returnValue(False)
        ver = d[u'redis_version']
        self.redis_version = ver
        ver_list = [int(x) for x in ver.split(u'.')]
        if len(ver_list) < 2:
            defer.returnValue(False)
        if len(ver_list) == 2:
            ver_list.append(0)
        if ver_list[0] > major:
            defer.returnValue(True)
        elif ver_list[0] == major:
            if ver_list[1] > minor:
                defer.returnValue(True)
            elif ver_list[1] == minor:
                if ver_list[2] >= patch:
                    defer.returnValue(True)
        defer.returnValue(False)


class Redis26CheckMixin(RedisVersionCheckMixin):
    def is_redis_2_6(self):
        """
        Returns true if the Redis version >= 2.6
        """
        return self.checkVersion(2, 6)

    def _skipCheck(self):
        if not self.redis_2_6:
            skipMsg = "Redis version < 2.6 (found version: %s)"
            raise unittest.SkipTest(skipMsg % self.redis_version)
