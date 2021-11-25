package message

import "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"

type StreamMessage interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(data []byte) error
	SetPublishingId(id int64)
	GetPublishingId() int64
	HasPublishingId() bool
	GetData() [][]byte
	GetMessageProperties() *amqp.MessageProperties
}
