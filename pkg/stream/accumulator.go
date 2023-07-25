package stream

import (
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"sync"
)

type messageAccumulator struct {
	rwMutex sync.RWMutex
	next    int
	maxLen  int
	queue   []raw.Message
}

// UNSAFE!! Make sure to acquire a write lock before calling this function
func (m *messageAccumulator) clear() {
	m.next = 0
	// FIXME this leaks
	m.queue = m.queue[:0]
}

func (m *messageAccumulator) push(msg raw.Message) error {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	if m.next == m.maxLen {
		return ErrQueueFull
	}
	//m.queue[m.next] = msg
	m.queue = append(m.queue, msg)
	m.next += 1
	return nil
}

func (m *messageAccumulator) isEmpty() bool {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()
	return m.next == 0
}
