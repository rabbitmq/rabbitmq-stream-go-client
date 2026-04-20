# Changelog

All notable changes to this project will be documented in this file.

## [[1.8.0](https://github.com/rabbitmq/rabbitmq-stream-go-client/releases/tag/v1.8.0)]

## 1.8.0 - 2026-04-20
- [Release 1.8.0](https://github.com/rabbitmq/rabbitmq-stream-go-client/releases/tag/v1.8.0)

### Breaking Changes
- Remove golang compatibility 1.24 by @Gsantomaggio in [#483](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/483)

### Enhancements
- Add `.github/release.yml` for release changelog by @Gsantomaggio in [#477](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/477)
- Update perftest to 1.26 by @Gsantomaggio in [#484](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/484)
- Correct multiple bugs and code quality issues in Coordinator by @Gsantomaggio in [#485](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/485)
- Correct bugs and code quality issues in `client.go` and `socket.go` by @Gsantomaggio in [#486](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/486)
- Add colors to perfTest by @Gsantomaggio in [#489](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/489)
- Add loadbalancer options by @Gsantomaggio in [#490](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/490)

### Bug Fixes
- Prevent panic on send to closed `chunkForConsumer` channel by @bithckr in [#481](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/481)

### Dependency Updates
- Bump github.com/klauspost/compress from 1.18.4 to 1.18.5 by @dependabot[bot] in [#479](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/479)
- Bump go.opentelemetry.io/otel from 1.41.0 to 1.42.0 by @dependabot[bot] in [#472](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/472)
- Bump golang.org/x/text from 0.34.0 to 0.35.0 by @dependabot[bot] in [#475](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/475)
- Bump go.opentelemetry.io/otel from 1.42.0 to 1.43.0 by @dependabot[bot] in [#487](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/487)
- Bump go.opentelemetry.io/otel/sdk from 1.40.0 to 1.43.0 in `/examples/metrics` by @dependabot[bot] in [#491](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/491)
- Bump golang.org/x/text from 0.35.0 to 0.36.0 by @dependabot[bot] in [#492](https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/492)

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
