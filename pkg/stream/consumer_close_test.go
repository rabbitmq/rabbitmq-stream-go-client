package stream

// Unit tests for Consumer shutdown correctness.
// These tests exercise the concurrent close/dispatch paths without requiring
// a running RabbitMQ broker.

import (
	"sync"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// makeCloseTestConsumer builds a minimal Consumer using the coordinator,
// mirroring the pattern used in coordinator_test.go. The client is never
// connected to a broker, so these tests run entirely in-process.
func makeCloseTestConsumer() (*Consumer, *Client) {
	client := newClient(connectionParameters{
		connectionName:    "close-test-client",
		broker:            nil,
		tcpParameters:     nil,
		saslConfiguration: nil,
		rpcTimeout:        defaultSocketCallTimeout,
	})
	consumer, err := client.coordinator.NewConsumer(nil, NewConsumerOptions(), nil)
	Expect(err).NotTo(HaveOccurred())
	consumer.client = client
	return consumer, client
}

var _ = Describe("Consumer shutdown", func() {

	// -----------------------------------------------------------------------
	// 1. Original panic: "send on closed channel".
	//    Goroutines replicate server_frame.go's TOCTOU pattern:
	//      if consumer.getStatus() == open { client.enqueueConsumerChunk(...) }
	//    close() closes closeCh while senders may be blocked on the connection queue.
	//    Expected: no panics. FAILS on unpatched main, PASSES after fix.
	// -----------------------------------------------------------------------
	It("concurrent chunk dispatch and close does not panic", func() {
		consumer, client := makeCloseTestConsumer()

		const senders = 50
		var panicCount atomic.Int32
		var wg sync.WaitGroup
		wg.Add(senders)

		// Barrier so all goroutines fire as simultaneously as possible.
		start := make(chan struct{})

		for range senders {
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						panicCount.Add(1)
					}
				}()
				<-start
				for range 200 {
					// Replicate server_frame.go: status check then enqueue on the client.
					if consumer.getStatus() == open {
						_ = client.enqueueConsumerChunk(consumer.GetID(), chunkInfo{})
					}
				}
			}()
		}

		close(start) // release all senders at once
		consumer.close(Event{Reason: SocketClosed})
		wg.Wait()

		Expect(panicCount.Load()).To(Equal(int32(0)))
	})

	// -----------------------------------------------------------------------
	// 2. Original panic: "close of closed channel".
	//    Without idempotency, concurrent calls to close() both reach
	//    close(response.data) → double-close panic.
	//    Expected: no panics. FAILS on unpatched main, PASSES after fix.
	// -----------------------------------------------------------------------
	It("concurrent close calls are idempotent and do not panic", func() {
		consumer, _ := makeCloseTestConsumer()

		const closers = 20
		var panicCount atomic.Int32
		var wg sync.WaitGroup
		wg.Add(closers)
		start := make(chan struct{})

		for range closers {
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						panicCount.Add(1)
					}
				}()
				<-start
				consumer.close(Event{Reason: SocketClosed})
			}()
		}

		close(start)
		wg.Wait()

		Expect(panicCount.Load()).To(Equal(int32(0)))
	})

	// -----------------------------------------------------------------------
	// 3. Consumer status is closed after close().
	// -----------------------------------------------------------------------
	It("consumer status is closed after close()", func() {
		consumer, _ := makeCloseTestConsumer()
		consumer.close(Event{Reason: SocketClosed})
		Expect(consumer.getStatus()).To(Equal(closed))
	})

	// -----------------------------------------------------------------------
	// 4. After close, the client must reject further chunk enqueues for the subscription.
	// -----------------------------------------------------------------------
	It("enqueue after consumer close returns false", func() {
		consumer, client := makeCloseTestConsumer()
		consumer.close(Event{Reason: SocketClosed})
		Expect(client.enqueueConsumerChunk(consumer.GetID(), chunkInfo{})).To(BeFalse())
	})
})
