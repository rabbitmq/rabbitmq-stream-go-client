package stream

import (
	"context"
	"errors"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"golang.org/x/exp/slog"
	"sync"
	"time"
)

var (
	ErrQueueFull              = errors.New("outgoing queue is full")
	ErrInvalidProducerOptions = errors.New("invalid producer options")
)

// producer opts

type ProducerOpts struct {
	ClientProvidedName string
	MaxInFlight        int
	MaxMessagesPerSend int
	ConfirmTimeout     time.Duration
}

func (p *ProducerOpts) validate() error {
	if p.MaxInFlight == 0 {
		// assume user did not provide a value, set default value
		p.MaxInFlight = 1_000
	}
	if p.MaxInFlight < 0 {
		return fmt.Errorf("%w: max in flight messages cannot be negative", ErrInvalidProducerOptions)
	}

	if p.MaxMessagesPerSend == 0 {
		// assume user did not provide a value, set default
		p.MaxMessagesPerSend = 100
	}
	if p.MaxMessagesPerSend < 1 {
		return fmt.Errorf("%w: max messages per send cannot be less than 1")
	}

	if p.ConfirmTimeout == 0 {
		// assume user did not provide a value, set default
		p.ConfirmTimeout = time.Second * 30
	}
	if p.ConfirmTimeout < time.Second {
		return fmt.Errorf("%w: confirmation timeout cannot be less than 1 second", ErrInvalidProducerOptions)
	}

	return nil
}

// producer

// by default make 1 producer -> 1 connection

type smartProducer struct {
	log                   *slog.Logger
	doneCh                chan struct{}
	accumulator           messageAccumulator
	publishingIdGenerator autoIncrementNumber[uint64]
	sendTicker            *time.Ticker
	// client is shared among all producers in the connection. Smart producer must
	// ensure its accessed in a thread-safe manner
	clientMu    *sync.Mutex
	client      raw.Clienter
	publisherId uint8
	stream      string
	internalId  int
}

func newSmartProducer(streamName string, opts *ProducerOpts, logger *slog.Logger) *smartProducer {
	p := &smartProducer{
		log:    logger.WithGroup("producer"),
		doneCh: make(chan struct{}),
		accumulator: messageAccumulator{
			rwMutex: sync.RWMutex{},
			next:    0,
			queue:   make([]raw.Message, 0, opts.MaxMessagesPerSend),
			maxLen:  opts.MaxMessagesPerSend,
		},
		publishingIdGenerator: autoIncrementNumber[uint64]{},
		sendTicker:            nil,
		stream:                streamName,
	}
	return p
}

func (s *smartProducer) start() {
	if s.sendTicker != nil {
		// smart producer is already started
		return
	}
	s.sendTicker = time.NewTicker(time.Millisecond * 100)
	go func() {
		for {
			select {
			case <-s.doneCh:
				s.sendTicker.Stop()
				return
			case <-s.sendTicker.C:
				if s.accumulator.isEmpty() {
					continue
				}

				err := s.doSend(context.TODO())
				if err != nil {
					s.log.Error("error publishing messages", slog.Any("error", err))
				}
			}
		}
	}()
}

func (s *smartProducer) doSend(ctx context.Context) error {
	s.accumulator.rwMutex.Lock()
	defer s.accumulator.rwMutex.Unlock()

	if len(s.accumulator.queue) == 0 {
		return nil
	}

	s.clientMu.Lock()
	err := s.client.Send(ctx, s.publisherId, s.accumulator.queue)
	s.clientMu.Unlock()
	if err != nil {
		return err
	}
	s.accumulator.clear()
	return nil
}

func (s *smartProducer) Send(msg amqp.Message) error {
	m := raw.NewPublishingMessage(s.publishingIdGenerator.next(), &msg)
	err := s.accumulator.push(m)
	if err != nil && errors.Is(err, ErrQueueFull) {
		s.log.Debug("outgoing queue is full, forcing send")
		err := s.doSend(context.TODO())
		if err != nil {
			return err
		}
		//TODO use a sync.Condition to sync on queue accepting messages
		return s.accumulator.push(m)
	} else if err != nil {
		return err
	}
	return nil
}

func (s *smartProducer) SendWithId(publishingId uint64, msg amqp.Message) error {
	//TODO implement me
	panic("implement me")
}

func (s *smartProducer) SendBatch(publishingId uint64, batch []amqp.Message) error {
	//TODO implement me
	panic("implement me")
}

func (s *smartProducer) GetLastPublishedId() int64 {
	//TODO implement me
	panic("implement me")
}

func (s *smartProducer) Close() error {
	// TODO use a sync.Once to avoid closing the channel multiple times and panicking
	close(s.doneCh)
	// TODO drain remaining queue
	// FIXME do not close raw client
	//return s.client.Close(context.TODO())
	return nil
}

func (s *smartProducer) id() int {
	return s.internalId
}

func (s *smartProducer) done() <-chan struct{} {
	return s.doneCh
}
