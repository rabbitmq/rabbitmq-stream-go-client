//go:build rabbitmq.stream.test

package stream

import "github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"

func (e *Environment) SetLocatorRawClient(host string, c raw.Clienter) {
	e.locators[host] = &locator{Client: c}
}
