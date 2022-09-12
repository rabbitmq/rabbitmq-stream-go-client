package stream

import (
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
)

type Client interface {
	writeCommand(request internal.CommandWrite) error
	peerProperties() error
	Connect(brokers []Broker) error
}
