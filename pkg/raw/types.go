package raw

import (
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/common"
	"io"
)

// Message is the interface for the publishing message
// it creates a message with a publishingId and a Serializer
// The message doesn't contain the publishingId this is why we need this interface
type Message interface {
	io.WriterTo
	SetPublishingId(publishingId uint64)
	PublishingId() uint64
	SetMessage(message common.Serializer)
	Message() common.Serializer
}
