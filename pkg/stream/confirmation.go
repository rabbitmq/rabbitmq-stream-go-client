package stream

import (
	"fmt"
	"sync"
)

type confirmationTracker struct {
	*sync.Mutex
	messages map[uint64]PublishingMessage
}

func (p *confirmationTracker) add(m PublishingMessage) {
	p.Lock()
	defer p.Unlock()
	id := m.PublishingId()
	p.messages[id] = m
}

func (p *confirmationTracker) addMany(m ...PublishingMessage) {
	if len(m) == 0 {
		return
	}

	p.Lock()
	defer p.Unlock()
	for _, message := range m {
		p.messages[message.PublishingId()] = message
	}
}

func (p *confirmationTracker) confirm(id uint64) (PublishingMessage, error) {
	p.Lock()
	defer p.Unlock()
	pm, found := p.messages[id]
	if !found {
		return nil, fmt.Errorf("%w: publishingID %d", ErrUntrackedConfirmation, id)
	}
	delete(p.messages, id)
	return pm, nil
}
