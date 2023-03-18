# Changelog

## Release 1.4.8 (2023-03-18)

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
