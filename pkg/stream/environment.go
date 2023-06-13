package stream

import (
	"context"
	"errors"
	"fmt"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
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

		// TODO: add retry logic
		if err != nil {
			continue
		}

		e.locators = append(e.locators, l)
	}
	// FIXME: return last error
	return nil
}

func (e *Environment) pickLocator(n int) *locator {
	if n > len(e.locators) {
		return e.locators[0]
	}
	return e.locators[n]
}

// CreateStream with name and given options.
func (e *Environment) CreateStream(ctx context.Context, name string, opts StreamOptions) error {
	var lastError error

	// TODO:💡try to make the attempts generic, so that we pass a function and it is executed
	// 	with N attempts. The challenge might be passing the parameters
	rn := rand.Intn(100)
	n := len(e.locators)
	for i := 0; i < n; i++ {
		ctxCreate, cancel := context.WithTimeout(ctx, DefaultTimeout)
		// context cancellation is checked in the raw layer
		l := e.pickLocator((i + rn) % n)
		lastError = l.createStream(ctxCreate, name, streamOptionsToRawStreamConfiguration(opts))
		cancel()

		if lastError == nil {
			return lastError
		}

		// give up on non-retryable errors
		if errors.Is(lastError, raw.ErrStreamAlreadyExists) {
			return lastError
		}
	}

	return fmt.Errorf("locator operation failed: %w", lastError)
}
