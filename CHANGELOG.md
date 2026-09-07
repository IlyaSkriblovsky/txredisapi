# Changelog

## Release 1.6.0 (UNRELEASED)

### Bugfixes

- Hostnames are now resolved with `getaddrinfo()`, through Twisted's
  `HostnameEndpoint`, instead of `socket.gethostbyname()`. Servers reachable
  over IPv6 only (a name with an AAAA record and no A record) can be connected
  to now (#161)

- `disconnect()` now also cancels a scheduled reconnection and a connection
  attempt in progress

- With `reconnect=False`, the Deferred returned by `Connection()` and
  `ConnectionPool()` errbacks when the connection can't be established instead
  of never firing at all

- The reconnection delay is reset after a successful connection instead of
  growing up to `maxDelay` and staying there forever

### Incompatible changes

- Connections are maintained by
  `twisted.application.internet.ClientService` now, and `RedisFactory` is no
  longer a `ReconnectingClientFactory`. Code connecting a factory on its own
  with `reactor.connectTCP()` or `twisted.application.internet.TCPClient` has
  to use `factory.startConnecting(endpoint)` or its own
  `ClientService(endpoint, factory)` instead - see `examples/subscriber.py`

- `factory.continueTrying = False` is replaced by `factory.stopTrying()`;
  `retry()`, `resetDelay()` and `delay` are gone. `maxDelay`, `initialDelay`,
  `factor` and `jitter` still configure the backoff and are used by the new
  `RedisFactory.retryDelay()`

- `SentinelConnectionFactory.try_to_connect()` is gone: the address of the
  master or of a slave is discovered by the endpoint before every attempt.
  Subclasses overriding `try_to_connect()`, `clientConnectionFailed()` or
  `clientConnectionLost()` have no effect anymore

- `connectTimeout` applies to each resolved address of a hostname rather than
  to the connection attempt as a whole

- Twisted 18.7.0 or newer is required

---

## Release 1.5.0 (2026-07-27)

### Incompatible change

- Dropped support for Python <3.5

---

## Release 1.4.12 (2026-05-06)

### Features

- Add `username` parameter to all connection functions for Redis 6+ ACL
  authentication (`AUTH username password`)

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
