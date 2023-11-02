package stream

import (
	"context"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/common"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"log"
	"sync"
	"time"
)

// Default values for ProducerOptions
const (
	DefaultBatchPublishingDelay = time.Millisecond * 100
	DefaultMaxInFlight          = 10_000
	DefaultMaxBufferedMessages  = 100
	DefaultConfirmTimeout       = time.Second * 10
	DefaultEnqueueTimeout       = time.Second * 10
)

type ProducerOptions struct {
	// The maximum number of unconfirmed outbound messages. Producer.Send will start
	// blocking when the limit is reached. Only accepts positive integers. Any negative
	// value will be set to the default.
	MaxInFlight int
	// The maximum number of messages to accumulate before sending them to the
	// broker. Only accepts positive integers. Any negative value will be set to the
	// default.
	MaxBufferedMessages int
	// Accumulating period to send a batch of messages.
	BatchPublishingDelay time.Duration
	// Time before failing a message enqueue. This acts as a timeout to Send a
	// message. If the message is not accumulated before this timeout, the publish
	// operation will be considered failed.
	EnqueueTimeout time.Duration
	// Handler function for publish confirmations. The function receives a pointer
	// to a message confirmation. The handler should inspect to confirmation to
	// determine whether the message was received, timed out, or else. See also
	// ConfirmationStatus.
	//
	// The following code logs the confirmations received:
	//
	//		opt := &ProducerOptions{
	//			ConfirmationHandler: func(c *MessageConfirmation) {
	//				log.Printf("Received message confirmation: ID: %d, Status: %s", c.PublishingId(), c.Status())
	//				// some code
	//			},
	//		}
	//
	// The handler is invoked synchronously by the background routine handling
	// confirms. It is critical to not perform long or expensive operations inside
	// the handler, as it will stall the progress of the background routine, and
	// subsequent invocations of confirmation handlers.
	ConfirmationHandler func(*MessageConfirmation)
	// Time before the client calls the confirm callback to signal outstanding
	// unconfirmed messages timed out.
	//
	// Set to -1 to disable confirmation timeouts. Any other negative value
	// is invalid, and it will be set to the default.
	ConfirmTimeout time.Duration
	// Used internally. Must be set by the producer manager
	stream string
}

func (p *ProducerOptions) validate() {
	if p.MaxInFlight <= 0 {
		p.MaxInFlight = DefaultMaxInFlight
	}
	if p.MaxBufferedMessages <= 0 {
		p.MaxBufferedMessages = DefaultMaxBufferedMessages
	}
	if p.BatchPublishingDelay == 0 {
		p.BatchPublishingDelay = DefaultBatchPublishingDelay
	}
	if p.EnqueueTimeout < 0 {
		p.EnqueueTimeout = DefaultEnqueueTimeout
	}
	if p.ConfirmTimeout == 0 || p.ConfirmTimeout < -1 {
		p.ConfirmTimeout = DefaultConfirmTimeout
	}
}

func (p *ProducerOptions) DeepCopy() *ProducerOptions {
	r := new(ProducerOptions)
	r.MaxInFlight = p.MaxInFlight
	r.MaxBufferedMessages = p.MaxBufferedMessages
	r.BatchPublishingDelay = p.BatchPublishingDelay
	r.EnqueueTimeout = p.EnqueueTimeout
	r.ConfirmationHandler = p.ConfirmationHandler
	r.ConfirmTimeout = p.ConfirmTimeout
	r.stream = p.stream
	return r
}

type standardProducer struct {
	m           sync.Mutex
	publisherId uint8
	status      status
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
	closeCallback      func(int) error
	unconfirmedMessage *confirmationTracker
	// this channel is used by the producer manager to send confirmation notifications
	// the end-user does not have access to this channel
	confirmedPublish chan *publishConfirmOrError
}

func newStandardProducer(publisherId uint8, rawClient raw.Clienter, clientM *sync.Mutex, opts *ProducerOptions) *standardProducer {
	opts.validate()
	p := &standardProducer{
		m:               sync.Mutex{},
		publisherId:     publisherId,
		status:          open,
		rawClient:       rawClient,
		rawClientMu:     clientM,
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

	if opts.ConfirmTimeout != -1 {
		go p.confirmationTimeoutLoop()
	}

	return p
}

func (s *standardProducer) shutdown() {
	s.cancel()
	s.destructor.Do(func() {
		close(s.done)
	})
}

func (s *standardProducer) setCloseCallback(f func(int) error) {
	s.closeCallback = f
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
	return len(s.unconfirmedMessage.unconfirmedMessagesSemaphore) != s.opts.MaxInFlight
}

func (s *standardProducer) isOpen() bool {
	s.m.Lock()
	defer s.m.Unlock()
	return s.status == open
}

func (s *standardProducer) sendLoopAsync(ctx context.Context) {
	var publishingDelay time.Duration
	if s.opts.BatchPublishingDelay == 0 {
		// important to make this check because NewTicker(0) panics
		publishingDelay = DefaultBatchPublishingDelay
	} else {
		publishingDelay = s.opts.BatchPublishingDelay
	}

	t := time.NewTicker(publishingDelay)
	for {
		select {
		case <-t.C:
			// send
			ctx2, cancel := maybeApplyDefaultTimeout(ctx)
			err := s.doSend(ctx2)
			if err != nil {
				// FIXME: log error using slog.logger
				log.Printf("error sending: %v", err)
			}
			cancel()
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

func (s *standardProducer) confirmationTimeoutLoop() {
	t := time.NewTicker(s.opts.ConfirmTimeout)

	for {
		select {
		case now := <-t.C:
			expiredIds := s.unconfirmedMessage.idsBefore(now.Add(-s.opts.ConfirmTimeout))
			for _, id := range expiredIds {
				confirmation, err := s.unconfirmedMessage.timeoutMessage(id)
				if err != nil {
					// TODO: log error
					continue
				}
				if s.opts.ConfirmationHandler != nil {
					s.opts.ConfirmationHandler(confirmation)
				}
			}
		case <-s.done:
			return
		}
	}
}

// Public API

// Send an AMQP 1.0 message asynchronously. Messages are accumulated in a buffer,
// and sent after a delay, or when the buffer becomes full, whichever happens
// first.
func (s *standardProducer) Send(ctx context.Context, msg amqp.Message) error {
	if !s.isOpen() {
		return ErrProducerClosed
	}

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
//
// This function blocks if the number of messages in flight (i.e. not confirmed
// by the broker) is greater than len(messages). This function will observe
// context cancellation
func (s *standardProducer) SendBatch(ctx context.Context, messages []amqp.Message) error {
	if !s.isOpen() {
		return ErrProducerClosed
	}

	if len(messages) == 0 {
		return ErrEmptyBatch
	}

	if n := len(messages); n > s.opts.MaxInFlight {
		return fmt.Errorf("%w: max %d, batch len %d", ErrBatchTooLarge, s.opts.MaxInFlight, n)
	}

	var pMsgs = make([]common.PublishingMessager, 0, len(messages))
	for i := 0; i < len(messages); i++ {
		// checking context cancellation here because adding a message
		// confirmation is blocking
		if isContextCancelled(ctx) {
			return ctx.Err()
		}

		next := s.publishingIdSeq.next()
		pMsgs = append(pMsgs, raw.NewPublishingMessage(next, &messages[i]))

		s.unconfirmedMessage.add(&MessageConfirmation{
			publishingId: next,
			messages:     []amqp.Message{messages[i]},
			status:       WaitingConfirmation,
			insert:       time.Now(),
			stream:       s.opts.stream,
		})
	}

	s.rawClientMu.Lock()
	defer s.rawClientMu.Unlock()

	return s.rawClient.Send(ctx, s.publisherId, pMsgs)
}

// SendWithId always returns an error in the standard producer because the
// publishing ID is tracked internally.
//
// TODO: revisit this. We could let users set their own publishing IDs, and very strongly
//
//	advise to use one or the other, but not both
func (s *standardProducer) SendWithId(_ context.Context, _ uint64, _ amqp.Message) error {
	return fmt.Errorf("%w: standard producer does not support sending with ID", ErrUnsupportedOperation)
}

// Close the producer. After calling this function, the producer is considered finished
// and not expected to send anymore messages. Subsequent attempts to send messages will
// result in an error.
func (s *standardProducer) Close() {
	// TODO: should we wait for pending confirmations?
	s.m.Lock()
	if s.status == closing || s.status == closed {
		s.m.Unlock()
		return
	}
	s.status = closing
	s.m.Unlock()

	s.shutdown()
	if s.closeCallback != nil {
		_ = s.closeCallback(int(s.publisherId))
		// TODO: log error
	}
	s.m.Lock()
	s.status = closed
	s.m.Unlock()
}
