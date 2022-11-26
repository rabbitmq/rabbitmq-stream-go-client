package common

import (
	"bufio"
	"context"
)

type Clienter interface {
	Connect(ctx context.Context) error
	DeclareStream(ctx context.Context, stream string, configuration StreamConfiguration) error
	DeleteStream(ctx context.Context, stream string) error
	DeclarePublisher(ctx context.Context, publisherId uint8, publisherReference string, stream string) error
	Send(ctx context.Context, publisherId uint8, messages []StreamerMessage) error
	IsOpen() bool
	Close(ctx context.Context) error
}

// TODO: godocs
type StreamConfiguration = map[string]string

func NewConfiguration(name string) map[string]string {
	c := make(StreamConfiguration)
	c["name"] = name
	return c
}

type StreamerMessage interface {
	Write(writer *bufio.Writer) (int, error)
	SetPublishingId(publishingId uint64)
	GetPublishingId() uint64
	SetBody(body []byte)
	GetBody() []byte
}
