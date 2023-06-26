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

// DeleteStream with given name.
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
