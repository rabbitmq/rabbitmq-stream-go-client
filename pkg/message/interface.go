package message

import "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"

// StreamMessage is the interface that wraps the basic methods to interact with a message
// in the context of a stream.
// Currently, the StreamMessage interface is implemented by the amqp.Message struct.
// The implementations are not meant to be thread-safe.
type StreamMessage interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(data []byte) error
	SetPublishingId(id int64)
	GetPublishingId() int64
	HasPublishingId() bool
	GetData() [][]byte
	GetMessageProperties() *amqp.MessageProperties
	GetMessageAnnotations() amqp.Annotations
	GetApplicationProperties() map[string]interface{}

	// GetMessageHeader GetAMQPValue read only values see: rabbitmq-stream-go-client/issues/128
	GetMessageHeader() *amqp.MessageHeader
	GetAMQPValue() interface{}
}
