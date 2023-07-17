//go:build rabbitmq.stream.test

package stream

import (
	"context"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"golang.org/x/exp/slog"
	"time"
)

type discardHandler struct{}

func (d discardHandler) Enabled(context.Context, slog.Level) bool {
	return false
}

func (d discardHandler) Handle(context.Context, slog.Record) error {
	// do nothing
	return nil
}

func (d discardHandler) WithAttrs([]slog.Attr) slog.Handler {
	return d
}

func (d discardHandler) WithGroup(string) slog.Handler {
	return d
}

func (e *Environment) AppendLocatorRawClient(c raw.Clienter) {
	e.locators = append(e.locators, &locator{
		client: c,
		isSet:  true,
		log:    slog.New(&discardHandler{}),
		backOffPolicy: func(int) time.Duration {
			return time.Millisecond * 10
		},
	})
}

// SetServerVersion sets the version in the server properties in the locator's
// raw client configuration. Important to use it in tests for functions that
// require 3.11+
func (e *Environment) SetServerVersion(v string) {
	for _, l := range e.locators {
		l.rawClientConf.SetServerProperties("version", v)
	}
}

func (e *Environment) SetBackoffPolicy(f func(int) time.Duration) {
	e.backOffPolicy = f
}
