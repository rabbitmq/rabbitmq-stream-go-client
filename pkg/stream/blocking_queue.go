package stream

import "time"

type BlockingQueue[T any] struct {
	queue    chan T
	capacity int
}

// NewBlockingQueue initializes a new BlockingQueue with the given capacity
func NewBlockingQueue[T any](capacity int) *BlockingQueue[T] {
	return &BlockingQueue[T]{
		queue:    make(chan T, capacity),
		capacity: capacity,
	}
}

// Enqueue adds an item to the queue, blocking if the queue is full
func (bq *BlockingQueue[T]) Enqueue(item T) {
	bq.queue <- item // This will block if the queue is full
}

// Dequeue removes an item from the queue with a timeout
func (bq *BlockingQueue[T]) Dequeue(timeout time.Duration) T {
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
