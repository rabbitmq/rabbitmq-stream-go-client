package stream

import (
	"errors"
	"sync/atomic"
	"time"
)

var ErrBlockingQueueStopped = errors.New("blocking queue stopped")

type BlockingQueue[T any] struct {
	queue    chan T
	capacity int
	status   int32
}

// NewBlockingQueue initializes a new BlockingQueue with the given capacity
func NewBlockingQueue[T any](capacity int) *BlockingQueue[T] {
	return &BlockingQueue[T]{
		queue:    make(chan T, capacity),
		capacity: capacity,
		status:   0,
	}
}

// Enqueue adds an item to the queue, blocking if the queue is full
func (bq *BlockingQueue[T]) Enqueue(item T) error {
	if bq.IsStopped() {
		return ErrBlockingQueueStopped
	}
	bq.queue <- item // This will block if the queue is full
	return nil
}

// Dequeue removes an item from the queue with a timeout
func (bq *BlockingQueue[T]) Dequeue(timeout time.Duration) T {
	if bq.IsStopped() {
		var zeroValue T // Zero value of type T
		return zeroValue
	}
	select {
	case item := <-bq.queue:
		return item
	case <-time.After(timeout):
		var zeroValue T // Zero value of type T
		return zeroValue
	}
}

func (bq *BlockingQueue[T]) Size() int {
	return len(bq.queue)
}

func (bq *BlockingQueue[T]) IsEmpty() bool {
	return len(bq.queue) == 0
}

// Stop stops the queue from accepting new items
// but allows the existing items to be processed
// Stop is different from Close in that it allows the
// existing items to be processed.
// That avoids the need to drain the queue before closing it.
func (bq *BlockingQueue[T]) Stop() {
	atomic.StoreInt32(&bq.status, 1)
}

func (bq *BlockingQueue[T]) Close() {
	if bq.IsStopped() {
		atomic.StoreInt32(&bq.status, 2)
		close(bq.queue)
	}
}

func (bq *BlockingQueue[T]) IsStopped() bool {
	return atomic.LoadInt32(&bq.status) == 1 || atomic.LoadInt32(&bq.status) == 2
}
