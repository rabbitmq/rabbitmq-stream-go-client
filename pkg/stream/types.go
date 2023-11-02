package stream

import (
	"context"
	"errors"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/common"
	"time"
)

const (
	Kilobyte = 1_000
	Megabyte = Kilobyte * 1_000
	Gigabyte = Megabyte * 1_000
	Terabyte = Gigabyte * 1_000
)

var (
	ErrNoLocators            = errors.New("no locators configured")
	ErrUnsupportedOperation  = errors.New("unsupported operation")
	ErrBatchTooLarge         = errors.New("too many messages in batch")
	ErrEmptyBatch            = errors.New("batch list is empty")
	ErrUntrackedConfirmation = errors.New("message confirmation not tracked")
	ErrEnqueueTimeout        = errors.New("timed out queueing message")
	ErrMaxMessagesInFlight   = errors.New("maximum number of messages in flight")
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
	Send(ctx context.Context, msg amqp.Message) error
	SendBatch(ctx context.Context, messages []amqp.Message) error
	SendWithId(ctx context.Context, publishingId uint64, msg amqp.Message) error
}

type internalProducer interface {
	Producer
	close()
}

type Message = common.Message
type PublishingMessage = common.PublishingMessager
