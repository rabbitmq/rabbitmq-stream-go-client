package stream

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"sync"
	"time"
)

type ConfirmationStatus int

const (
	// WaitingConfirmation for a publishing message
	WaitingConfirmation ConfirmationStatus = iota
	// Confirmed and received message by the server
	Confirmed
	// ClientTimeout and gave up waiting for a confirmation
	ClientTimeout
	// NotAvailable Stream, often because stream was deleted
	NotAvailable
	// InternalError from the server
	InternalError
	// AccessRefused user did not have permissions to publish to the stream
	AccessRefused
	// PreconditionFailed means that certain conditions required to publish
	// were not met e.g. stream does not exist
	PreconditionFailed
	// PublisherDoesNotExist happens when the client tries to publish before the
	// publisher was registered and assigned an ID
	PublisherDoesNotExist
	// UndefinedError is for any other error
	UndefinedError
)

// TODO: docs
type MessageConfirmation struct {
	// publishing ID of the message/s in this confirmation
	publishingId uint64
	// list of AMQP messages in this confirmation. Its len will be 1, except with
	// sub-batch entries
	messages []amqp.Message
	// status of the confirmation
	status ConfirmationStatus
	// time when this message confirmation was created
	insert time.Time
	// stream name where the message/s were published to
	stream string
}

func (m *MessageConfirmation) PublishingId() uint64 {
	return m.publishingId
}

func (m *MessageConfirmation) Messages() []amqp.Message {
	return m.messages
}

func (m *MessageConfirmation) Status() ConfirmationStatus {
	return m.status
}

func (m *MessageConfirmation) Stream() string {
	return m.stream
}

// this type represents a publishing confirmation. In the smart layer, a
// confirmation can be a negative/error confirmation. It is a similar concept of
// a "nack" or negative ack in AMQP.
type publishConfirmOrError struct {
	publishingId uint64
	statusCode   uint16
}

// status translates a raw response code into a ConfirmationStatus
func (p *publishConfirmOrError) status() ConfirmationStatus {
	switch p.statusCode {
	case raw.ResponseCodeOK:
		return Confirmed
	case raw.ResponseCodeStreamDoesNotExist, raw.ResponseCodeStreamNotAvailable:
		return NotAvailable
	case raw.ResponseCodeInternalError:
		return InternalError
	case raw.ResponseCodeAccessRefused:
		return AccessRefused
	case raw.ResponseCodePublisherDoesNotExist:
		return PublisherDoesNotExist
	case raw.ResponseCodePreconditionFailed:
		return PreconditionFailed
	default:
		return UndefinedError
	}
}

type confirmationTracker struct {
	mapMu                        *sync.Mutex
	messages                     map[uint64]*MessageConfirmation
	unconfirmedMessagesSemaphore chan struct{}
	cap                          int
}

func newConfirmationTracker(capacity int) *confirmationTracker {
	return &confirmationTracker{
		mapMu:                        &sync.Mutex{},
		messages:                     make(map[uint64]*MessageConfirmation, capacity),
		unconfirmedMessagesSemaphore: make(chan struct{}, capacity),
		cap:                          capacity,
	}
}

// Adds a message confirmation to the tracker. It blocks if the maximum
// number of pending confirmations is reached.
func (p *confirmationTracker) add(m *MessageConfirmation) {
	p.mapMu.Lock()
	defer p.mapMu.Unlock()
	p.unconfirmedMessagesSemaphore <- struct{}{}

	id := m.PublishingId()
	p.messages[id] = m
}

// Tracks a pending message confirmation. It uses timeout to wait when the
// maximum number of pending message confirmations is reached. If timeout is 0,
// then it tries to add the message confirmation to the tracker, and returns an
// error if the tracker is full i.e. max number of pending confirmations is
// reached. It also returns an error if the timeout elapses and the message is
// not added to the tracker.
func (p *confirmationTracker) addWithTimeout(m *MessageConfirmation, timeout time.Duration) error {
	p.mapMu.Lock()
	defer p.mapMu.Unlock()

	if timeout == 0 {
		select {
		case p.unconfirmedMessagesSemaphore <- struct{}{}:
			id := m.PublishingId()
			p.messages[id] = m
			return nil
		default:
			return ErrMaxMessagesInFlight
		}
	}

	select {
	case p.unconfirmedMessagesSemaphore <- struct{}{}:
		id := m.PublishingId()
		p.messages[id] = m
		return nil
	case <-time.After(timeout):
		// maybe it's better to return ErrMaxMessagesInFlight
		return ErrEnqueueTimeout
	}
}

// adds a batch of messages to track confirmation. This function
// blocks if:
//
//	len(m) + len(confirmationTracker.messages) > cap
func (p *confirmationTracker) addMany(m ...*MessageConfirmation) {
	if len(m) == 0 {
		return
	}

	p.mapMu.Lock()
	defer p.mapMu.Unlock()

	// we should not block here if there's sufficient capacity
	// by acquiring the map mutex, we ensure that other functions
	// don't "steal" or "race" semaphore's permits
	for i := 0; i < len(m); i++ {
		p.unconfirmedMessagesSemaphore <- struct{}{}
	}
	for _, message := range m {
		p.messages[message.PublishingId()] = message
	}
}

// Removes the message confirmation for a publishing id. It returns
// an error if the message was not tracked.
func (p *confirmationTracker) confirm(id uint64) (*MessageConfirmation, error) {
	p.mapMu.Lock()
	defer p.mapMu.Unlock()
	pm, found := p.messages[id]
	if !found {
		return nil, fmt.Errorf("%w: publishingID %d", ErrUntrackedConfirmation, id)
	}
	<-p.unconfirmedMessagesSemaphore
	delete(p.messages, id)
	return pm, nil
}
