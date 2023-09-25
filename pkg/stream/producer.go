package stream

import (
	"context"
	"errors"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/common"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"sync"
	"time"
)

var (
	errMessageBufferFull = errors.New("message buffer is full")
)

type ProducerOptions struct {
	MaxInFlight         int
	MaxBufferedMessages int
}

type standardProducer struct {
	publisherId     uint8
	rawClient       raw.Clienter
	rawClientMu     *sync.Mutex
	publishingIdSeq autoIncrementingSequence[uint64]
	opts            ProducerOptions
	// buffer mutex
	bufferMu      *sync.Mutex
	messageBuffer []common.PublishingMessager
	retryDuration backoffDurationFunc
	done          chan struct{}
	cancel        context.CancelFunc
	destructor    sync.Once
}

func newStandardProducer(publisherId uint8, rawClient raw.Clienter, opts ProducerOptions) *standardProducer {
	if opts.MaxInFlight <= 0 {
		opts.MaxInFlight = 10_000
	}
	if opts.MaxBufferedMessages <= 0 {
		opts.MaxBufferedMessages = 100
	}
	p := &standardProducer{
		publisherId:     publisherId,
		rawClient:       rawClient,
		rawClientMu:     &sync.Mutex{},
		publishingIdSeq: autoIncrementingSequence[uint64]{},
		opts:            opts,
		bufferMu:        &sync.Mutex{},
		messageBuffer:   make([]common.PublishingMessager, 0, opts.MaxBufferedMessages),
		retryDuration: func(i int) time.Duration {
			return time.Second * (1 << i)
		},
		done: make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	go p.sendLoopAsync(ctx)
	return p
}

func (s *standardProducer) close() {
	s.cancel()
	s.destructor.Do(func() {
		close(s.done)
	})
}

// push is not thread-safe. A bufferMu lock must be acquired before calling this function.
func (s *standardProducer) push(publishingId uint64, message Message) error {
	if len(s.messageBuffer) >= s.opts.MaxBufferedMessages {
		return errMessageBufferFull
	}
	s.messageBuffer = append(s.messageBuffer, raw.NewPublishingMessage(publishingId, message))
	return nil
}

// synchronously sends the messages accumulated in the message buffer. If sending
// is successful, it clears the message buffer.
func (s *standardProducer) doSend(ctx context.Context) error {
	s.bufferMu.Lock()
	s.rawClientMu.Lock()
	defer s.rawClientMu.Unlock()
	defer s.bufferMu.Unlock()

	if len(s.messageBuffer) == 0 {
		return nil
	}

	var err error
	for i := 0; i < maxAttempt; i++ {
		err = s.rawClient.Send(ctx, s.publisherId, s.messageBuffer)
		if isNonRetryableError(err) {
			return err
		}
		if err == nil {
			break
		}

		<-time.After(s.retryDuration(i))
	}

	if err != nil {
		return err
	}

	for i := 0; i < len(s.messageBuffer); i++ {
		// FIXME: keep track of pending messages in a different list
		s.messageBuffer[i] = nil
	}
	s.messageBuffer = s.messageBuffer[:0]

	return nil
}

func (s *standardProducer) sendLoopAsync(ctx context.Context) {
	// FIXME: configurable ticker
	t := time.NewTicker(time.Millisecond * 300)

	for {
		select {
		case <-t.C:
			// send
			err := s.doSend(ctx)
			if err != nil {
				// log error
				panic(err)
			}
		case <-s.done:
		case <-ctx.Done():
			// exit
			t.Stop()
			return
		}
	}
}

// Public API

func (s *standardProducer) Send(ctx context.Context, msg amqp.Message) error {
	//TODO implement me
	s.bufferMu.Lock()
	if len(s.messageBuffer) == s.opts.MaxBufferedMessages {
		s.bufferMu.Unlock()
		if err := s.doSend(ctx); err != nil {
			return err
		}
	}

	_ = s.push(s.publishingIdSeq.next(), &msg)
	s.bufferMu.Unlock()

	return nil
}

// SendBatch synchronously sends messages to the broker. Each messages gets
// a publishing ID assigned automatically.
func (s *standardProducer) SendBatch(ctx context.Context, messages []amqp.Message) error {
	if len(messages) == 0 {
		return ErrEmptyBatch
	}

	if n := len(messages); n > s.opts.MaxInFlight {
		return fmt.Errorf("%w: max %d, batch len %d", ErrBatchTooLarge, s.opts.MaxInFlight, n)
	}

	var pMsgs = make([]common.PublishingMessager, 0, len(messages))
	for i := 0; i < len(messages); i++ {
		pMsgs = append(pMsgs, raw.NewPublishingMessage(s.publishingIdSeq.next(), &messages[i]))
	}

	s.rawClientMu.Lock()
	defer s.rawClientMu.Unlock()

	return s.rawClient.Send(ctx, s.publisherId, pMsgs)
}

// SendWithId always returns an error in the standard producer because the
// publishing ID is tracked internally.
func (s *standardProducer) SendWithId(_ context.Context, _ uint64, _ amqp.Message) error {
	return fmt.Errorf("%w: standard producer does not support sending with ID", ErrUnsupportedOperation)
}

func (s *standardProducer) GetLastPublishedId() uint64 {
	//TODO implement me
	panic("implement me")
}
