package raw

import (
	"bufio"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/common"
)

type PublishingMessage struct {
	publishingId uint64
	message      common.StreamerMessage
}

func (m *PublishingMessage) SetPublishingId(publishingId uint64) {
	m.publishingId = publishingId
}

func (m *PublishingMessage) GetPublishingId() uint64 {
	return m.publishingId
}

func (m *PublishingMessage) SetMessage(message common.StreamerMessage) {
	m.message = message
}

func (m *PublishingMessage) GetMessage() common.StreamerMessage {
	return m.message
}

func (m *PublishingMessage) Write(writer *bufio.Writer) (int, error) {
	written, err := internal.WriteMany(writer, m.publishingId)
	if err != nil {
		return written, err
	}
	writtenM, err := m.message.Write(writer)
	if err != nil {
		return writtenM, err
	}
	return written + writtenM, nil
}

func NewPublishingMessage(publishingId uint64, message common.StreamerMessage) *PublishingMessage {
	return &PublishingMessage{publishingId: publishingId, message: message}
}
