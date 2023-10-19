package stream

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"sync"
	"time"
)

type ConfirmationStatus int

const (
	// WaitingConfirmation for a publishing message
	WaitingConfirmation ConfirmationStatus = 0
	// Confirmed and received message by the server
	Confirmed ConfirmationStatus = 1
	// ClientTimeout and gave up waiting for a confirmation
	ClientTimeout ConfirmationStatus = 2
	// NotAvailable Stream, often because stream was deleted
	NotAvailable ConfirmationStatus = 6
	// InternalError from the server
	InternalError ConfirmationStatus = 15
	// TODO: do we need this?
	AccessRefused         ConfirmationStatus = 16
	PreconditionFailed    ConfirmationStatus = 17
	PublisherDoesNotExist ConfirmationStatus = 18
	UndefinedError        ConfirmationStatus = 200
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

func (p *confirmationTracker) add(m *MessageConfirmation) {
	p.mapMu.Lock()
	defer p.mapMu.Unlock()
	p.unconfirmedMessagesSemaphore <- struct{}{}

	id := m.PublishingId()
	p.messages[id] = m
}

func (p *confirmationTracker) addWithTimeout(m *MessageConfirmation, timeout time.Duration) error {
	p.mapMu.Lock()
	defer p.mapMu.Unlock()

	select {
	case p.unconfirmedMessagesSemaphore <- struct{}{}:
		id := m.PublishingId()
		p.messages[id] = m
		return nil
	case <-time.After(timeout):
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
