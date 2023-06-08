//go:build rabbitmq.stream.test

package stream

import (
	"context"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
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
		Client: c,
		isSet:  true,
		log:    slog.New(&discardHandler{}),
		backOffPolicy: func(int) time.Duration {
			return time.Millisecond * 10
		},
	})
}

func (e *Environment) SetBackoffPolicy(f func(int) time.Duration) {
	e.backOffPolicy = f
}
