package stream

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
)

var ErrBlockingQueueStopped = errors.New("blocking queue stopped")

// BlockingQueue is a bounded multi-producer queue with a bulk consumer side.
// It is a mutex-protected slice rather than a channel: with a channel the
// consumer pays a lock round-trip per item, which contends heavily with the
// producers. Here the consumer takes a whole batch per lock acquisition.
type BlockingQueue[T any] struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	items    []T
	head     int
	capacity int
	status   atomic.Int32 // 0 running, 1 stopped, 2 closed
}

// NewBlockingQueue initializes a new BlockingQueue with the given capacity
func NewBlockingQueue[T any](capacity int) *BlockingQueue[T] {
	bq := &BlockingQueue[T]{capacity: capacity}
	bq.notEmpty = sync.NewCond(&bq.mu)
	bq.notFull = sync.NewCond(&bq.mu)
	return bq
}

// Enqueue adds an item to the queue, blocking if the queue is full
func (bq *BlockingQueue[T]) Enqueue(item T) error {
	bq.mu.Lock()
	for len(bq.items)-bq.head >= bq.capacity && !bq.IsStopped() {
		bq.notFull.Wait()
	}
	if bq.IsStopped() {
		bq.mu.Unlock()
		return ErrBlockingQueueStopped
	}
	bq.items = append(bq.items, item)
	bq.mu.Unlock()
	bq.notEmpty.Signal()
	return nil
}

// DequeueBatch blocks until at least one item is available and moves up to max
// items into dst (which is truncated first). It returns nil once the queue is
// closed and drained.
func (bq *BlockingQueue[T]) DequeueBatch(dst []T, max int) []T {
	bq.mu.Lock()
	for len(bq.items) == bq.head {
		if bq.status.Load() == 2 {
			bq.mu.Unlock()
			return nil
		}
		bq.notEmpty.Wait()
	}
	n := min(len(bq.items)-bq.head, max)
	dst = append(dst[:0], bq.items[bq.head:bq.head+n]...)
	clear(bq.items[bq.head : bq.head+n])
	bq.head += n
	if bq.head == len(bq.items) {
		bq.items = bq.items[:0]
		bq.head = 0
	}
	bq.mu.Unlock()
	bq.notFull.Broadcast()
	return dst
}

func (bq *BlockingQueue[T]) Size() int {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return len(bq.items) - bq.head
}

func (bq *BlockingQueue[T]) IsEmpty() bool {
	return bq.Size() == 0
}

// Stop stops the queue from accepting new items
// but allows some pending items.
// Stop is different from Close in that it allows the
// existing items to be processed.
// Drain the queue to be sure there are not pending messages
func (bq *BlockingQueue[T]) Stop() []T {
	bq.status.Store(1)
	// release the senders blocked on a full queue
	bq.mu.Lock()
	bq.mu.Unlock() //nolint:staticcheck // the lock orders us after any Enqueue between its check and Wait
	bq.notFull.Broadcast()

	// drain the queue. To be sure there are not pending messages
	// in the queue and return to the caller the remaining pending messages
	var msgInQueue []T
	for {
		bq.mu.Lock()
		if len(bq.items) > bq.head {
			msgInQueue = append(msgInQueue, bq.items[bq.head:]...)
			clear(bq.items[bq.head:])
			bq.items = bq.items[:0]
			bq.head = 0
			bq.mu.Unlock()
			continue
		}
		bq.mu.Unlock()
		// grace period for the consumer, as before
		time.Sleep(10 * time.Millisecond)
		bq.mu.Lock()
		empty := len(bq.items) == bq.head
		bq.mu.Unlock()
		if empty {
			break
		}
	}
	logs.LogDebug("BlockingQueue stopped")
	return msgInQueue
}

func (bq *BlockingQueue[T]) Close() {
	if bq.IsStopped() {
		bq.status.Store(2)
		bq.mu.Lock()
		bq.mu.Unlock() //nolint:staticcheck // see Stop
		bq.notEmpty.Broadcast()
	}
}

func (bq *BlockingQueue[T]) IsStopped() bool {
	s := bq.status.Load()
	return s == 1 || s == 2
}
