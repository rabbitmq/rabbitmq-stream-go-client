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
	locators      []*locator
}

func NewEnvironment(configuration EnvironmentConfiguration) (*Environment, error) {
	e := &Environment{
		configuration: configuration,
		locators:      make([]*locator, 0, len(configuration.Uris)),
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

// CreateStream with name and given options.
func (e *Environment) CreateStream(ctx context.Context, name string, opts StreamOptions) error {
	for _, l := range e.locators {
		return l.Client.DeclareStream(ctx, name, streamOptionsToRawStreamConfiguration(opts))
	}
	// This should never happen
	return ErrNoLocators
}
