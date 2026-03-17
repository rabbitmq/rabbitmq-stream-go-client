# Changelog

All notable changes to this project will be documented in this file.

## [[1.7.1](https://github.com/rabbitmq/rabbitmq-stream-go-client/releases/tag/v1.7.1)]

## 1.7.1 - 2026-03-17
- [Release 1.7.1](https://github.com/rabbitmq/rabbitmq-stream-go-client/releases/tag/v1.7.1)

### Enhancements
- Add IClient interface by @Gsantomaggio in [#470](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/470)
- Add IEntity interface for Producer and Consumer struct by @Gsantomaggio in [#471](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/471)
- Improve send by @Gsantomaggio in [#476](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/476)

### Dependency Updates
- Bump go.opentelemetry.io/otel/metric from 1.40.0 to 1.41.0 by @dependabot[bot] in [#468](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/468)

### Bug Fix
- Fix data race on Producer.closeHandler between close() and NotifyClose() by @Gsantomaggio in [#474](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/474)

## [[1.7.0](https://github.com/rabbitmq/rabbitmq-stream-go-client/releases/tag/v1.7.0)]

## 1.7.0 - 2026-02-16
- [Release 1.7.0](https://github.com/rabbitmq/rabbitmq-stream-go-client/releases/tag/v1.7.0)

### Enhancements
- Add OpenTelemetry metrics instrumentation by @Zerpet in [#461](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/461)

### Dependency Updates
- Bump github.com/klauspost/compress from 1.18.2 to 1.18.3 by @dependabot[bot] in [#460](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/460)
- Bump go.opentelemetry.io/otel from 1.39.0 to 1.40.0 by @dependabot[bot] in [#463](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/463)
- Bump golang.org/x/text from 0.33.0 to 0.34.0 by @dependabot[bot] in [#465](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/465)
- Bump github.com/klauspost/compress from 1.18.3 to 1.18.4 by @dependabot[bot] in [#466](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/466)

## [[1.6.3](https://github.com/rabbitmq/rabbitmq-stream-go-client/releases/tag/v1.6.3)]

## 1.6.3 - 2026-01-14
- [Release 1.6.3](https://github.com/rabbitmq/rabbitmq-stream-go-client/releases/tag/v1.6.3)

### Fix
- Fix super stream reconnection by @Gsantomaggio in [#456](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/456)
- Fix Load Balancer configuration drops connections by @Gsantomaggio in [#456](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/456)

### Dependency Updates
- Bump golang.org/x/text from 0.31.0 to 0.33.0 by @dependabot[bot] in [#457](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/457)
