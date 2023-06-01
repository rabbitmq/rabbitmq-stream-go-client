//go:build rabbitmq.stream.test

package stream

import (
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	"time"
)

func (e *Environment) AppendLocatorRawClient(c raw.Clienter) {
	e.locators = append(e.locators, &locator{Client: c, isSet: true})
}

func (e *Environment) SetBackoffPolicy(f func(int) time.Duration) {
	e.backOffPolicy = f
}
