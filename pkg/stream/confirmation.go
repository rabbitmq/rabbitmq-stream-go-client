package stream

import (
	"fmt"
	"sync"
	"time"
)

type confirmationTracker struct {
	mapMu                        *sync.Mutex
	messages                     map[uint64]PublishingMessage
	unconfirmedMessagesSemaphore chan struct{}
	cap                          int
}

func newConfirmationTracker(capacity int) *confirmationTracker {
	return &confirmationTracker{
		mapMu:                        &sync.Mutex{},
		messages:                     make(map[uint64]PublishingMessage, capacity),
		unconfirmedMessagesSemaphore: make(chan struct{}, capacity),
		cap:                          capacity,
	}
}

func (p *confirmationTracker) add(m PublishingMessage) {
	p.mapMu.Lock()
	defer p.mapMu.Unlock()
	p.unconfirmedMessagesSemaphore <- struct{}{}

	id := m.PublishingId()
	p.messages[id] = m
}

func (p *confirmationTracker) addWithTimeout(m PublishingMessage, timeout time.Duration) error {
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
func (p *confirmationTracker) addMany(m ...PublishingMessage) {
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

func (p *confirmationTracker) confirm(id uint64) (PublishingMessage, error) {
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
