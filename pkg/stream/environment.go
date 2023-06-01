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

func NewEnvironment(configuration EnvironmentConfiguration) (*Environment, error) {
	e := &Environment{
		configuration: configuration,
		locators:      make([]*locator, 0, len(configuration.Uris)),
	}

	e.backOffPolicy = func(attempt int) time.Duration {
		return time.Second * time.Duration(attempt<<1)
	}

	if !configuration.LazyInitialization {
		return e, e.start()
	}

	return e, nil
}

func (e *Environment) start() error {
	for i, uri := range e.configuration.Uris {
		c, err := raw.NewClientConfiguration(uri)
		if err != nil {
			return err
		}

		c.SetConnectionName(fmt.Sprintf("%s-locator-%d", e.configuration.Id, i))

		l := newLocator(*c)
		ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
		err = l.connect(ctx)
		cancel()

		if err != nil {
			continue
		}

		e.locators = append(e.locators, l)
	}

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
	logger := raw.LoggerFromCtxOrDiscard(ctx).WithGroup("stream.environment")

	var executed bool
	var lastError error

	// TODO:💡try to make the attempts generic, so that we pass a function and it is executed
	// 	with N attempts. The challenge might be passing the parameters
	rn := rand.Intn(100)
	n := len(e.locators)
	for i := 0; i < n; i++ {
		for attempt, maxAttempt := 0, 3; attempt < maxAttempt; {
			// context cancellation is checked in the raw layer
			lastError = e.pickLocator((i+rn)%n).
				Client.DeclareStream(ctx, name, streamOptionsToRawStreamConfiguration(opts))
			if lastError == nil {
				executed = true
				break
			}

			logger.Error("error creating stream", slog.Any("error", lastError), slog.String("name", name), slog.Int("attempt", attempt))

			attempt++

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(e.backOffPolicy(attempt)):
			}
		}

		if executed {
			return nil
		}
	}
	// This should never happen
	return fmt.Errorf("locator operation failed: %w", lastError)
}
