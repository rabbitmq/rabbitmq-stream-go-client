package raw

import (
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/internal"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/common"
	"io"
)

type PublishingMessage struct {
	publishingId uint64
	message      common.Message
}

func (m *PublishingMessage) SetPublishingId(publishingId uint64) {
	m.publishingId = publishingId
}

func (m *PublishingMessage) PublishingId() uint64 {
	return m.publishingId
}

func (m *PublishingMessage) SetMessage(message common.Message) {
	m.message = message
}

func (m *PublishingMessage) Message() common.Message {
	return m.message
}

func (m *PublishingMessage) WriteTo(writer io.Writer) (int64, error) {
	binary, err := m.Message().MarshalBinary()
	if err != nil {
		return 0, err
	}
	written, err := internal.WriteMany(writer, m.publishingId,
		len(binary), binary)
	if err != nil {
		return int64(written), err
	}

	return int64(written), nil
}

func NewPublishingMessage(publishingId uint64, message common.Message) *PublishingMessage {
	return &PublishingMessage{publishingId: publishingId, message: message}
}
