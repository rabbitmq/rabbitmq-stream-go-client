package common

import "context"

type Clienter interface {
	Connect(ctx context.Context) error
	DeclareStream(ctx context.Context, stream string, configuration StreamConfiguration) error
	DeleteStream(ctx context.Context, stream string) error
	IsOpen() bool
	Close(ctx context.Context) error
}

// TODO: godocs
type StreamConfiguration map[string]string

func NewConfiguration(name string) map[string]string {
	c := make(StreamConfiguration)
	c["name"] = name
	return c
}
