package stream

import (
	"context"
	"errors"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/common"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"log"
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
	// Time before enqueueing of a message fail when the maximum number of
	// unconfirmed is reached.
	EnqueueTimeout time.Duration
	// TODO: docs
	ConfirmationHandler func(confirmation *MessageConfirmation)
	// Used internally. Must be set by the producer manager
	stream string
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
	if p.EnqueueTimeout < 0 {
		p.EnqueueTimeout = time.Second * 10
	}
}

type standardProducer struct {
	publisherId uint8
	// shared Raw Client connection among all clients in the same manager
	rawClient raw.Clienter
	// this mutex must be shared among all components that have access to this
	// rawClient. The Raw Client is not thread-safe and its access must be
	// synchronised
	rawClientMu        *sync.Mutex
	publishingIdSeq    autoIncrementingSequence[uint64]
	opts               *ProducerOptions
	accumulator        *messageAccumulator
	retryDuration      backoffDurationFunc
	done               chan struct{}
	cancel             context.CancelFunc
	destructor         sync.Once
	unconfirmedMessage *confirmationTracker
	// this channel is used by the producer manager to send confirmation notifications
	// the end-user does not have access to this channel
	confirmedPublish chan *publishConfirmOrError
}

func newStandardProducer(publisherId uint8, rawClient raw.Clienter, opts *ProducerOptions) *standardProducer {
	opts.validate()
	p := &standardProducer{
		publisherId:     publisherId,
		rawClient:       rawClient,
		rawClientMu:     &sync.Mutex{}, // FIXME: this has to come as argument. this mutex must be shared among all users of the client
		publishingIdSeq: autoIncrementingSequence[uint64]{},
		opts:            opts,
		accumulator:     newMessageAccumulator(opts.MaxBufferedMessages),
		retryDuration: func(i int) time.Duration {
			return time.Second * (1 << i)
		},
		done:               make(chan struct{}),
		unconfirmedMessage: newConfirmationTracker(opts.MaxInFlight),
		confirmedPublish:   make(chan *publishConfirmOrError),
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	go p.sendLoopAsync(ctx)
	go p.confirmationListenerLoop()
	return p
}

func (s *standardProducer) close() {
	s.cancel()
	s.destructor.Do(func() {
		close(s.done)
	})
}

// synchronously sends the messages accumulated in the message buffer. If sending
// is successful, it clears the message buffer.
func (s *standardProducer) doSend(ctx context.Context) error {
	s.rawClientMu.Lock()
	defer s.rawClientMu.Unlock()

	if !s.canSend() {
		// TODO: maybe log a message
		return nil
	}

	// TODO: explore if we can have this buffer in a sync.Pool
	messages := make([]PublishingMessage, 0, s.opts.MaxBufferedMessages)
	for batchSize := 0; batchSize < s.opts.MaxBufferedMessages && s.canSend(); batchSize++ {
		pm := s.accumulator.get()
		if pm == nil {
			break
		}
		messages = append(messages, pm)

		// the smart layer only supports AMQP 1.0 message formats
		// we should never panic here
		m := pm.Message().(*amqp.Message)
		err := s.unconfirmedMessage.addWithTimeout(&MessageConfirmation{
			publishingId: pm.PublishingId(),
			messages:     []amqp.Message{*m},
			status:       WaitingConfirmation,
			insert:       time.Now(),
			stream:       s.opts.stream,
		}, s.opts.EnqueueTimeout)
		if err != nil {
			// TODO: log error
			break
		}
	}

	if len(messages) == 0 {
		// this case happens when pending confirms == max in-flight messages
		// we return so that we don't send an empty Publish frame
		return nil
	}

	err := s.rawClient.Send(ctx, s.publisherId, messages)
	if err != nil {
		return err
	}

	return nil
}

func (s *standardProducer) canSend() bool {
	return len(s.unconfirmedMessage.unconfirmedMessagesSemaphore) != s.opts.MaxInFlight &&
		!s.accumulator.isEmpty()
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
			err := s.doSend(ctx)
			if err != nil {
				// FIXME: log error using logger
				//panic(err)
				log.Printf("error sending: %v", err)
			}
		case <-s.done:
			// exit
			t.Stop()
			return
		case <-ctx.Done():
			// exit
			t.Stop()
			return
		}
	}
}

func (s *standardProducer) confirmationListenerLoop() {
	for {
		select {
		case <-s.done:
			return
		case confirmOrError := <-s.confirmedPublish:
			msgConfirm, err := s.unconfirmedMessage.confirm(confirmOrError.publishingId)
			if err != nil {
				// TODO: log the error instead
				panic(err)
			}
			msgConfirm.status = confirmOrError.status()
			if s.opts.ConfirmationHandler != nil {
				s.opts.ConfirmationHandler(msgConfirm)
			}
			// TODO: do we need an else { msgConfirm = nil } to ease the job of the GC?
		}
	}
}

// Public API

// Send an AMQP 1.0 message asynchronously. Messages are accumulated in a buffer,
// and sent after a delay, or when the buffer becomes full, whichever happens
// first.
func (s *standardProducer) Send(ctx context.Context, msg amqp.Message) error {
	//TODO implement me
	var send bool
	if s.opts.EnqueueTimeout != 0 {
		var err error
		send, err = s.accumulator.addWithTimeout(raw.NewPublishingMessage(s.publishingIdSeq.next(), &msg), s.opts.EnqueueTimeout)
		if err != nil {
			return fmt.Errorf("error sending message: %w", err)
		}
	} else {
		var err error
		send, err = s.accumulator.add(raw.NewPublishingMessage(s.publishingIdSeq.next(), &msg))
		if err != nil {
			return fmt.Errorf("error sending message: %w", err)
		}
	}

	if send {
		err := s.doSend(ctx)
		if err != nil {
			// at this point, we are tracking messages as unconfirmed,
			// however, it is very likely they have never reached the broker
			// a background worker will have to time out the confirmation
			// This situation makes the infinite enqueue timeout dangerous
			return err
		}
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
