package common

import (
	"bufio"
	"context"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"
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
	Write(writer *bufio.Writer) (int, error)
	SetBody(body []byte)
	GetBody() []byte
}

type PublishingMessager interface {
	Write(writer *bufio.Writer) (int, error)
	SetPublishingId(publishingId uint64)
	GetPublishingId() uint64
	SetMessage(message StreamerMessage)
	GetMessage() StreamerMessage
}
