package stream

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"time"
)

const (
	Kilobyte = 1_000
	Megabyte = Kilobyte * 1_000
	Gigabyte = Megabyte * 1_000
	Terabyte = Gigabyte * 1_000
)

var (
	ErrNoLocators           = fmt.Errorf("no locators configured")
	ErrUnsupportedOperation = fmt.Errorf("unsupported operation")
)

type ByteCapacity uint64

func (b ByteCapacity) String() string {
	return fmt.Sprintf("%d", b)
}

// StreamOptions is an alias for backwards compatibility with v1 of this client.
//
// Deprecated: use CreateStreamOptions. This alias is kept for backwards compatibility
type StreamOptions = CreateStreamOptions

type CreateStreamOptions struct {
	MaxAge         time.Duration
	MaxLength      ByteCapacity
	MaxSegmentSize ByteCapacity
}

type Producer interface {
	Send(m amqp.Message) error
	SendWithId(publishingId uint64, m amqp.Message) error
	/*
		TODO remove this comment
		Equivalent to .NET
		public ValueTask Send(List<(ulong, Message)> messages);
	*/
	SendBatch(publishingId uint64, batch []amqp.Message) error
	GetLastPublishedId() int64
	Close() error
	id() int
	done() <-chan struct{}
}
