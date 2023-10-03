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

const (
	defaultBatchPublishingDelay = time.Millisecond * 100
	defaultMaxInFlight          = 10_000
	maxBufferedMessages         = 100
)

type ProducerOptions struct {
	// The maximum number of unconfirmed outbound messages. Producer.Send will start
	// blocking when the limit is reached.
	MaxInFlight int
	// The maximum number of messages to accumulate before sending them to the
	// broker.
	MaxBufferedMessages int
	// Period to send a batch of messages.
	BatchPublishingDelay time.Duration
}

func (p *ProducerOptions) validate() {
	if p.MaxInFlight <= 0 {
		p.MaxInFlight = defaultMaxInFlight
	}
	if p.MaxBufferedMessages <= 0 {
		p.MaxBufferedMessages = maxBufferedMessages
	}
	if p.BatchPublishingDelay == 0 {
		p.BatchPublishingDelay = defaultBatchPublishingDelay
	}
}

type standardProducer struct {
	publisherId     uint8
	rawClient       raw.Clienter
	rawClientMu     *sync.Mutex
	publishingIdSeq autoIncrementingSequence[uint64]
	opts            *ProducerOptions
	// buffer mutex
	bufferMu           *sync.Mutex
	messageBuffer      []PublishingMessage
	retryDuration      backoffDurationFunc
	done               chan struct{}
	cancel             context.CancelFunc
	destructor         sync.Once
	unconfirmedMessage confirmationTracker
	confirmedPublish   chan uint64
}

func newStandardProducer(publisherId uint8, rawClient raw.Clienter, opts *ProducerOptions) *standardProducer {
	opts.validate()
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
		done:             make(chan struct{}),
		confirmedPublish: make(chan uint64),
		unconfirmedMessage: confirmationTracker{
			Mutex:    &sync.Mutex{},
			messages: make(map[uint64]PublishingMessage, opts.MaxInFlight),
		},
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
// is successful, it clears the message buffer. The caller MUST hold a lock
// on the buffer mutex. Calling this function without a lock on buffer mutex is
// undefined behaviour
func (s *standardProducer) doSend(ctx context.Context) error {
	s.rawClientMu.Lock()
	defer s.rawClientMu.Unlock()

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
	var publishingDelay time.Duration
	if s.opts.BatchPublishingDelay == 0 {
		// important to make this check because NewTicker(0) panics
		publishingDelay = defaultBatchPublishingDelay
	} else {
		publishingDelay = s.opts.BatchPublishingDelay
	}

	t := time.NewTicker(publishingDelay)
	for {
		select {
		case <-t.C:
			// send
			s.bufferMu.Lock()
			err := s.doSend(ctx)
			s.bufferMu.Unlock()
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

// Send an AMQP 1.0 message asynchronously. Messages are accumulated in a buffer,
// and sent after a delay, or when the buffer becomes full, whichever happens
// first.
func (s *standardProducer) Send(ctx context.Context, msg amqp.Message) error {
	//TODO implement me
	s.bufferMu.Lock()
	defer s.bufferMu.Unlock()
	err := s.push(s.publishingIdSeq.next(), &msg)
	if err != nil && errors.Is(err, errMessageBufferFull) {
		return s.doSend(ctx)
	} else if err != nil {
		// this should never happen
		panic(err)
	}

	if len(s.messageBuffer) == s.opts.MaxBufferedMessages {
		return s.doSend(ctx)
	}

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
