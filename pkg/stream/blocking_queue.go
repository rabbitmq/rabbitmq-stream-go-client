package stream

import (
	"errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"sync/atomic"
	"time"
)

var ErrBlockingQueueStopped = errors.New("blocking queue stopped")

type BlockingQueue[T any] struct {
	queue      chan T
	status     int32
	capacity   int
	lastUpdate int64
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
	atomic.StoreInt64(&bq.lastUpdate, time.Now().UnixNano())
	bq.queue <- item
	return nil
}

func (bq *BlockingQueue[T]) GetChannel() chan T {
	return bq.queue
}

func (bq *BlockingQueue[T]) Size() int {
	return len(bq.queue)
}

func (bq *BlockingQueue[T]) IsEmpty() bool {
	return len(bq.queue) == 0
}

// Stop stops the queue from accepting new items
// but allows some pending items.
// Stop is different from Close in that it allows the
// existing items to be processed.
// Drain the queue to be sure there are not pending messages
func (bq *BlockingQueue[T]) Stop() {
	atomic.StoreInt32(&bq.status, 1)
	// drain the queue. To be sure there are not pending messages
	// in the queue.
	// it does not matter if we lose some messages here
	// since there is the unConfirmed map to handle the messages
	isActive := true
	for isActive {
		select {
		case <-bq.queue:
			// do nothing
		case <-time.After(10 * time.Millisecond):
			isActive = false
			return
		default:
			isActive = false
			return
		}
	}
	logs.LogDebug("BlockingQueue stopped")
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
