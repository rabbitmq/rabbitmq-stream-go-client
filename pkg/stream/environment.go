package stream

import (
	"context"
	"fmt"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	"golang.org/x/exp/slog"
	"math/rand"
	"time"
)

const (
	// DefaultTimeout in all network calls.
	DefaultTimeout = time.Second * 30
)

type Environment struct {
	configuration EnvironmentConfiguration
	locators      []*locator
	backOffPolicy func(int) time.Duration
}

func NewEnvironment(ctx context.Context, configuration EnvironmentConfiguration) (*Environment, error) {
	e := &Environment{
		configuration: configuration,
		locators:      make([]*locator, 0, len(configuration.Uris)),
	}

	e.backOffPolicy = func(attempt int) time.Duration {
		return time.Second * time.Duration(attempt<<1)
	}

	if !configuration.LazyInitialization {
		return e, e.start(ctx)
	}

	return e, nil
}

func (e *Environment) start(ctx context.Context) error {
	logger := raw.LoggerFromCtxOrDiscard(ctx)

	var lastConnectError error
	for i, uri := range e.configuration.Uris {
		c, err := raw.NewClientConfiguration(uri)
		if err != nil {
			return err
		}

		c.SetConnectionName(fmt.Sprintf("%s-locator-%d", e.configuration.Id, i))

		l := newLocator(*c, logger)
		// if ctx has a lower timeout, it will be used instead of DefaultTimeout
		// https://pkg.go.dev/context#WithDeadline
		ctx, cancel := context.WithTimeout(ctx, DefaultTimeout)
		err = l.connect(ctx)
		cancel()

		if err != nil {
			lastConnectError = err
			continue
		}

		e.locators = append(e.locators, l)
	}
	return lastConnectError
}

func (e *Environment) pickLocator(n int) *locator {
	if n > len(e.locators) {
		return e.locators[0]
	}
	return e.locators[n]
}

// # Public API

// CreateStream with name and given options.
func (e *Environment) CreateStream(ctx context.Context, name string, opts StreamOptions) error {
	logger := raw.LoggerFromCtxOrDiscard(ctx)

	rn := rand.Intn(100)
	n := len(e.locators)

	var lastError error
	for i := 0; i < n; i++ {
		l := e.pickLocator((i + rn) % n)

		if err := l.maybeInitializeLocator(); err != nil {
			logger.Error("locator not available", slog.Any("error", err))
			lastError = err
			continue
		}

		// TODO: refactor to use maybeApplyDefaultTimeout()
		ctxCreate, cancel := context.WithTimeout(ctx, DefaultTimeout)
		// context cancellation is checked in the raw layer
		res := l.locatorOperation((*locator).operationCreateStream, ctxCreate, name, streamOptionsToRawStreamConfiguration(opts))
		cancel()

		// check for nil first, otherwise type assertion will panic
		if res[0] == nil {
			return nil
		}
		lastError = res[0].(error)

		// give up on non-retryable errors
		if isNonRetryableError(lastError) {
			return lastError
		}
	}

	return fmt.Errorf("locator operation failed: %w", lastError)
}

// DeleteStream with given name. Returns an error if the stream does not exist, or if any
// unknown error occurs. The context may carry a [slog.Logger] to log operations and
// intermediate errors, if any.
//
// See also: [raw.NewContextWithLogger]
func (e *Environment) DeleteStream(ctx context.Context, name string) error {
	logger := raw.LoggerFromCtxOrDiscard(ctx)

	rn := rand.Intn(100)
	n := len(e.locators)

	var lastError error
	for i := 0; i < n; i++ {
		l := e.pickLocator((i + rn) % n)

		if err := l.maybeInitializeLocator(); err != nil {
			logger.Error("locator not available", slog.Any("error", err))
			lastError = err
			continue
		}

		opCtx, cancel := maybeApplyDefaultTimeout(ctx)
		result := l.locatorOperation((*locator).operationDeleteStream, opCtx, name)
		if cancel != nil {
			cancel()
		}

		if result[0] == nil {
			return nil
		}

		if err := result[0].(error); err != nil {
			lastError = err
			if isNonRetryableError(lastError) {
				return lastError
			}
			logger.Error("locator operation failed", slog.Any("error", lastError))
		}
	}
	return lastError
}

// Close the connection to RabbitMQ server. This function closes all connections
// to RabbitMQ gracefully. A graceful disconnection sends a close request to RabbitMQ
// and awaits a confirmation response. If there's any error closing a connection,
// the error is logged to a logger extracted from the context.
func (e *Environment) Close(ctx context.Context) {
	logger := raw.LoggerFromCtxOrDiscard(ctx).WithGroup("close")
	// TODO: shutdown producers/consumers
	for _, l := range e.locators {
		if l.isSet {
			err := l.client.Close(ctx)
			if err != nil {
				logger.Warn("error closing locator client", slog.Any("error", err))
			}
		}
	}
}

// QueryStreamStats queries the server for Stats from a given stream name.
// Stats available are 'first offset id' and 'committed chunk id'
//
// This command is available in RabbitMQ 3.11+
func (e *Environment) QueryStreamStats(ctx context.Context, name string) (Stats, error) {
	rn := rand.Intn(100)
	n := len(e.locators)

	var lastError error
	for i := 0; i < n; i++ {
		l := e.pickLocator((i + rn) % n)
		if err := l.maybeInitializeLocator(); err != nil {
			lastError = err
			// TODO: log error
			continue
		}

		result := l.locatorOperation((*locator).operationQueryStreamStats, ctx, name)
		if result[1] != nil {
			lastError = result[1].(error)
			if isNonRetryableError(lastError) {
				return Stats{-1, -1}, lastError
			}
			// TODO: log error
			continue
		}

		// TODO: log success at debug level
		stats := result[0].(map[string]int64)
		return Stats{stats["first_chunk_id"], stats["committed_chunk_id"]}, nil
	}
	return Stats{-1, -1}, lastError
}
