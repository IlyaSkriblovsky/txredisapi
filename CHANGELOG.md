# Changelog

## Release 1.4.6 (UNRELEASED)

### Bugfixes

- replyTimeout connection argument fixed. All query methods except `blpop()`,
  `brpop()`, `brpoplpush()` now raise `TimeoutError` if reply wasn't received
  within `replyTimeout` seconds.

- allow any commands to be sent via SubscriberProtocol

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
