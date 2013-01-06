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

import sys
import hashlib

import txredisapi as redis

from twisted.internet import defer
from twisted.trial import unittest
from twisted.internet import reactor
from twisted.python import failure

from .mixins import Redis26CheckMixin, REDIS_HOST, REDIS_PORT


class TestScripting(unittest.TestCase, Redis26CheckMixin):
    _SCRIPT = "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"  # From redis example

    @defer.inlineCallbacks
    def setUp(self):
        self.db = yield redis.Connection(REDIS_HOST, REDIS_PORT,
                                         reconnect=False)
        self.db1 = None
        self.redis_2_6 = yield self.is_redis_2_6()
        yield self.db.script_flush()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.db.disconnect()
        if self.db1 is not None:
            yield self.db1.disconnect()

    @defer.inlineCallbacks
    def test_eval(self):
        self._skipCheck()
        d = dict(key1="first", key2="second")
        r = yield self.db.eval(self._SCRIPT, **d)
        self._check_eval_result(d, r)
        r = yield self.db.eval("return 10")
        self.assertEqual(r, 10)
        r = yield self.db.eval("return {1,2,3.3333,'foo',nil,'bar'}")
        self.assertEqual(r, [1, 2, 3, "foo"])
        # Test the case where the hash is in script_hashes,
        # but redis doesn't have it
        h = self._hash_script(self._SCRIPT)
        yield self.db.script_flush()
        self.db.script_hashes.add(h)
        r = yield self.db.eval(self._SCRIPT, **d)
        self._check_eval_result(d, r)

    @defer.inlineCallbacks
    def test_eval_error(self):
        self._skipCheck()
        try:
            result = yield self.db.eval('return {err="My Error"}')
        except redis.ResponseError:
            pass
        except:
            raise self.failureException('%s raised instead of %s:\n %s'
                                        % (sys.exc_info()[0],
                                           'txredisapi.ResponseError',
                                           failure.Failure().getTraceback()))
        else:
            raise self.failureException('%s not raised (%r returned)'
                                        % ('txredisapi.ResponseError', result))

    @defer.inlineCallbacks
    def test_evalsha(self):
        self._skipCheck()
        r = yield self.db.eval(self._SCRIPT)
        h = self._hash_script(self._SCRIPT)
        r = yield self.db.evalsha(h)
        self._check_eval_result({}, r)

    @defer.inlineCallbacks
    def test_evalsha_error(self):
        self._skipCheck()
        h = self._hash_script(self._SCRIPT)
        try:
            result = yield self.db.evalsha(h)
        except redis.ScriptDoesNotExist:
            pass
        except:
            raise self.failureException('%s raised instead of %s:\n %s'
                                        % (sys.exc_info()[0],
                                           'txredisapi.ScriptDoesNotExist',
                                           failure.Failure().getTraceback()))
        else:
            raise self.failureException('%s not raised (%r returned)'
                                        % ('txredisapi.ResponseError', result))

    @defer.inlineCallbacks
    def test_script_load(self):
        self._skipCheck()
        h = self._hash_script(self._SCRIPT)
        r = yield self.db.script_exists(h)
        self.assertFalse(r)
        r = yield self.db.script_load(self._SCRIPT)
        self.assertEqual(r, h)
        r = yield self.db.script_exists(h)
        self.assertTrue(r)

    @defer.inlineCallbacks
    def test_script_exists(self):
        self._skipCheck()
        h = self._hash_script(self._SCRIPT)
        script1 = "return 1"
        h1 = self._hash_script(script1)
        r = yield self.db.script_exists(h)
        self.assertFalse(r)
        r = yield self.db.script_exists(h, h1)
        self.assertEqual(r, [False, False])
        yield self.db.script_load(script1)
        r = yield self.db.script_exists(h, h1)
        self.assertEqual(r, [False, True])
        yield self.db.script_load(self._SCRIPT)
        r = yield self.db.script_exists(h, h1)
        self.assertEqual(r, [True, True])

    @defer.inlineCallbacks
    def test_script_kill(self):
        self._skipCheck()
        try:
            result = yield self.db.script_kill()
        except redis.NoScriptRunning:
            pass
        except:
            raise self.failureException('%s raised instead of %s:\n %s'
                                        % (sys.exc_info()[0],
                                           'txredisapi.NoScriptRunning',
                                           failure.Failure().getTraceback()))
        else:
            raise self.failureException('%s not raised (%r returned)'
                                        % ('txredisapi.ResponseError', result))
        # Run an infinite loop script from one connection
        # and kill it from another.
        inf_loop = "while 1 do end"
        self.db1 = yield redis.Connection(REDIS_HOST, REDIS_PORT,
                                          reconnect=False)
        eval_deferred = self.db1.eval(inf_loop)
        reactor.iterate()
        r = yield self.db.script_kill()
        self.assertEqual(r, 'OK')
        try:
            result = yield eval_deferred
        except redis.ResponseError:
            pass
        except:
            raise self.failureException('%s raised instead of %s:\n %s'
                                        % (sys.exc_info()[0],
                                           'txredisapi.ResponseError',
                                           failure.Failure().getTraceback()))
        else:
            raise self.failureException('%s not raised (%r returned)'
                                        % ('txredisapi.ResponseError', result))

    def _check_eval_result(self, d, r):
        n = len(r)
        self.assertEqual(n, len(d) * 2)
        new_d = dict(zip(r[:n/2], r[n/2:]))
        for k, v in d.items():
            assert new_d[k] == v

    def _hash_script(self, script):
        return hashlib.sha1(script).hexdigest()
