package message

import "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"

type StreamMessage interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(data []byte) error
	SetPublishingId(id int64)
	GetPublishingId() int64
}

type AMQP10 struct {
	publishingId int64
	message      *amqp.Message
}

func NewMessage(data []byte) *AMQP10 {
	return &AMQP10{
		message:      amqp.NewMessage(data),
		publishingId: -1,
	}
}

func (amqp *AMQP10) SetPublishingId(id int64) {
	amqp.publishingId = id
}

func (amqp *AMQP10) GetPublishingId() int64 {
	return amqp.publishingId
}

func (amqp *AMQP10) MarshalBinary() ([]byte, error) {
	return amqp.message.MarshalBinary()
}

func (amqp *AMQP10) UnmarshalBinary(data []byte) error {
	return amqp.message.UnmarshalBinary(data)
}
