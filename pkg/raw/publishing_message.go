package raw

import (
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/common"
	"io"
)

type PublishingMessage struct {
	publishingId uint64
	message      common.StreamerMessage
}

func (m *PublishingMessage) SetPublishingId(publishingId uint64) {
	m.publishingId = publishingId
}

func (m *PublishingMessage) PublishingId() uint64 {
	return m.publishingId
}

func (m *PublishingMessage) SetMessage(message common.StreamerMessage) {
	m.message = message
}

func (m *PublishingMessage) Message() common.StreamerMessage {
	return m.message
}

func (m *PublishingMessage) WriteTo(writer io.Writer) (int64, error) {
	written, err := internal.WriteMany(writer, m.publishingId)
	if err != nil {
		return int64(written), err
	}
	writtenM, err := m.message.Write(writer)
	if err != nil {
		return int64(writtenM), err
	}
	return int64(written + writtenM), nil
}

func NewPublishingMessage(publishingId uint64, message common.StreamerMessage) *PublishingMessage {
	return &PublishingMessage{publishingId: publishingId, message: message}
}
