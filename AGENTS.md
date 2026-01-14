# AGENTS.md

This document provides guidance for AI agents and automated systems working with the RabbitMQ Stream Go Client codebase.

## Project Overview

This is a Go client library for [RabbitMQ Stream Queues](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream). The client provides high-level abstractions for producing and consuming messages from RabbitMQ streams.

### Key Components

- **`Environment`** (`pkg/stream/environment.go`): Main entry point that manages connections to broker(s)
- **`Producer`** (`pkg/stream/producer.go`): Interface for publishing messages to streams
- **`Consumer`** (`pkg/stream/consumer.go`): Interface for consuming messages from streams
- **`Client`** (`pkg/stream/client.go`): Low-level TCP connection handler
- **`SuperStreamProducer`** (`pkg/stream/super_stream_producer.go`): Producer for super streams (partitioned streams)
- **`SuperStreamConsumer`** (`pkg/stream/super_stream_consumer.go`): Consumer for super streams
- **`ha` package** (`pkg/ha/`): High-availability components (ReliableProducer, ReliableConsumer)


### Package Organization

- **`pkg/stream/`**: Core streaming functionality
- **`pkg/amqp/`**: AMQP 1.0 message encoding/decoding
- **`pkg/ha/`**: High-availability and reliability features
- **`pkg/logs/`**: Logging utilities
- **`pkg/message/`**: Message interface definitions
- **`examples/`**: Usage examples organized by feature
- **`perfTest/`**: Performance testing tool

### Thread Safety

- The library is generally thread-safe, but:
  - Messages are **NOT** thread-safe - do not share messages between goroutines
  - One producer/consumer per goroutine is recommended for best performance
  - Connections, producers, and consumers are designed to be long-lived

### Error Handling

- Use `errors.Is()` to check for specific error types (e.g., `stream.StreamAlreadyExists`)
- Network errors should be handled gracefully with retry logic
- Always check errors from `NewEnvironment()`, `NewProducer()`, and `NewConsumer()`

## Common Pitfalls to Avoid

1. **Holding mutexes during network I/O**: Always release mutexes before blocking operations
2. **Sharing messages between goroutines**: Messages are not thread-safe
3. **Creating/closing connections frequently**: Connections should be long-lived
4. **Ignoring close events**: Always handle `NotifyClose()` channels to detect disconnections
5. **Not handling reconnection**: Use `ReliableProducer`/`ReliableConsumer` if auto-reconnect is needed

## Testing Approach

- **Unit tests**: Located alongside source files (`*_test.go`)
- **Integration tests**: `pkg/integration_test/` - require running RabbitMQ server
- **Test helpers**: `pkg/test-helper/` - utilities for testing

When modifying code:
- Run existing tests: `go test ./...`
- Add tests for new functionality
- Integration tests may require Docker setup (see `compose/` directory)

## Code Style

- Follow standard Go conventions
- Use meaningful variable names
- Add comments for complex logic, especially around concurrency
- Document mutex usage patterns in comments
- Use `golangci-lint` for code quality (see `.github/workflows/golangci-lint.yml`)

### Producer/Consumer Lifecycle

- Producers and consumers don't auto-reconnect by default
- Use `ReliableProducer`/`ReliableConsumer` from `pkg/ha/` for auto-reconnect
- Always handle `NotifyClose()` events to detect disconnections

## Examples Reference

- **Basic usage**: `examples/getting_started/getting_started.go`
- **Reliable producer/consumer**: `examples/reliable_getting_started/reliable_getting_started.go`
- **Super streams**: `examples/reliable_super_stream_getting_started/reliable_super_stream_getting_started.go`
- **All examples**: See `examples/README.md`

## Building and Testing

- **Build**: Standard Go build (`go build`)
- **Test**: `go test ./...`
- **Lint**: Uses `golangci-lint` (configured in workflows)
- **Docker setup**: See `compose/` directory for local testing environment

## When Making Changes

1. **Review existing patterns**: Look for similar code patterns before introducing new ones
2. **Check mutex usage**: Ensure mutexes are not held during blocking operations
3. **Add tests**: Include unit tests for new functionality
4. **Update documentation**: Update relevant README files if API changes
5. **Check examples**: Ensure examples still work with changes
6. **Review CHANGELOG.md**: Add entries for user-facing changes

## Resources

- **Main README**: `README.md` - Comprehensive user documentation
- **Best Practices**: `best_practices/README.md` - Client usage guidelines
- **Changelog**: `CHANGELOG.md` - Release history
- **GitHub**: https://github.com/rabbitmq/rabbitmq-stream-go-client
