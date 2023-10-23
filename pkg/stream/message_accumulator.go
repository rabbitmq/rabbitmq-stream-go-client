package stream

import (
	"errors"
	"time"
)

type messageAccumulator struct {
	messages chan PublishingMessage
	cap      int
	// Accumulator is thread-safe because it uses a channel. A channel synchronises
	// itself
}

func newMessageAccumulator(capacity int) *messageAccumulator {
	return &messageAccumulator{cap: capacity, messages: make(chan PublishingMessage, capacity)}
}

// addWithTimeout a message to the queue. If the queue is full, it will wait at
// least, timeout duration before unblocking and returning an error. Returns true
// if this queue operation filled the message queue
func (m *messageAccumulator) addWithTimeout(message PublishingMessage, timeout time.Duration) (bool, error) {
	if message == nil {
		return false, errors.New("message can't be nil")
	}
	select {
	case m.messages <- message:
		return len(m.messages) == m.cap, nil
	case <-time.After(timeout):
		return false, ErrEnqueueTimeout
	}
}

// add queues a message into the message queue. It blocks if the queue is full.
// returns true if this queueing operation filled the queue to max capacity
func (m *messageAccumulator) add(message PublishingMessage) (bool, error) {
	if message == nil {
		return false, errors.New("message can't be nil")
	}

	m.messages <- message
	return len(m.messages) == m.cap, nil
}

// get the head of the queue or nil if it's empty
func (m *messageAccumulator) get() PublishingMessage {
	select {
	case pm := <-m.messages:
		return pm
	default:
		return nil
	}
}

func (m *messageAccumulator) isEmpty() bool {
	return len(m.messages) == 0
}
