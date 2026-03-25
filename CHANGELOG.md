# Changelog

## Release 1.4.12 (2026-04-??)

### Features

- Add `username` parameter to all connection functions for Redis 6+ ACL
  authentication (`AUTH username password`)

### Bugfixes

- Fix `RedisFactory.buildProtocol` not passing `username` to the protocol
  constructor, causing username-based auth to be silently ignored

### Documentation

- Correct connection function signatures in README (previously omitted
  `ssl_context_factory`, `connectTimeout`, `replyTimeout`, `convertNumbers`)
- Add strong recommendation to use keyword arguments due to the number of
  parameters and risk of silent misconfiguration with positional usage
- Fix test invocation command (`python -m twisted.trial tests`)

---
## Release 1.4.11 (2025-04-11)

### Bugfixes

- defer.returnValue() replaced with return to fix warnings when used with newer Twisted versions

---

## Release 1.4.10 (2023-07-06)

### Bugfixes

- Fix SubscriberProtocol to work with charset=None (#150)

---

## Release 1.4.9 (2023-03-18)

### Features

- SSL connection support

---

## Release 1.4.7 (2019-12-03)

### Bugfixes

- SentinelRedisProtocol.connectionMade not returns Deferred so subclasses might
  schedule interaction when connection is ready

---

## Release 1.4.6 (2019-11-20)

### Bugfixes

- Fixed authentication with Sentinel

- replyTimeout connection argument fixed. All query methods except `blpop()`,
  `brpop()`, `brpoplpush()` now raise `TimeoutError` if reply wasn't received
  within `replyTimeout` seconds.

- allow any commands to be sent via SubscriberProtocol

- Fixed bug in handling responses from Redis when MULTI is issued right after
  another bulk command (SMEMBERS for example)

---

## Release 1.4.5 (2017-11-08)

### Features

- Python 2.6 support

### Bugfixes

- Increasing memory consumption after many subscribe & unsubscribe commands

---

## Release 1.4.4 (2016-11-16)

### Features

- Redis Sentinel support
