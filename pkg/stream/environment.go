package stream

import (
	"context"
	"fmt"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	"time"
)

const (
	// DefaultTimeout in all network calls.
	DefaultTimeout = time.Second * 30
)

type Environment struct {
	configuration EnvironmentConfiguration
	locators      map[string]*locator
}

func NewEnvironment(configuration EnvironmentConfiguration) (*Environment, error) {
	e := &Environment{
		configuration: configuration,
		locators:      make(map[string]*locator, len(configuration.Uris)),
	}

	if !configuration.LazyInitialization {
		return e, e.start()
	}

	return e, nil
}

func (e *Environment) start() error {
	ctx := context.Background()

	for i, uri := range e.configuration.Uris {
		c, err := raw.NewClientConfiguration(uri)
		if err != nil {
			return err
		}

		c.SetConnectionName(fmt.Sprintf("%s-locator-%d", e.configuration.Id, i))

		dialCtx, cancel := context.WithTimeout(ctx, DefaultTimeout)
		client, err := raw.DialConfig(dialCtx, c)
		cancel()
		if err != nil {
			return err
		}

		locatorKey := fmt.Sprintf("%s:%d", e.configuration.Host, e.configuration.Port)
		if _, found := e.locators[locatorKey]; found {
			// TODO: handle duplicates
			panic("duplicate uri")
		}

		e.locators[locatorKey] = &locator{
			Client:          client,
			addressResolver: nil,
		}
	}

	return nil
}

// CreateStream with name and given options.
func (e *Environment) CreateStream(ctx context.Context, name string, opts StreamOptions) error {
	for _, l := range e.locators {
		return l.Client.DeclareStream(ctx, name, streamOptionsToRawStreamConfiguration(opts))
	}
	// This should never happen
	return ErrNoLocators
}
