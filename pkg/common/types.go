package common

import (
	"io"
)

type StreamerMessage interface {
	io.WriterTo
	SetBody(body []byte)
	Body() []byte
}

type PublishingMessager interface {
	io.WriterTo
	SetPublishingId(publishingId uint64)
	PublishingId() uint64
	SetMessage(message StreamerMessage)
	Message() StreamerMessage
}
