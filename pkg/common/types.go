package common

import (
	"context"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"
	"io"
)

type Clienter interface {
	Connect(ctx context.Context) error
	DeclareStream(ctx context.Context, stream string, configuration constants.StreamConfiguration) error
	DeleteStream(ctx context.Context, stream string) error
	DeclarePublisher(ctx context.Context, publisherId uint8, publisherReference string, stream string) error
	Send(ctx context.Context, publisherId uint8, messages []PublishingMessager) error
	DeletePublisher(ctx context.Context, publisherId uint8) error
	IsOpen() bool
	Close(ctx context.Context) error
}

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
