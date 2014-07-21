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

from twisted.internet import defer
from twisted.trial import unittest

import txredisapi as redis

from tests.mixins import REDIS_HOST, REDIS_PORT


class TestRedisConnections(unittest.TestCase):
    @defer.inlineCallbacks
    def testRedisPublish(self):
        db = yield redis.Connection(REDIS_HOST, REDIS_PORT, reconnect=False)

        for value in ("foo", "bar"):
            yield db.publish("test_publish", value)

        yield db.disconnect()
