package stream

import (
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("BlockingQueue", func() {

	It("dequeues in FIFO order and honours the max batch size", func() {
		bq := NewBlockingQueue[int](10)
		for i := 0; i < 7; i++ {
			Expect(bq.Enqueue(i)).To(Succeed())
		}
		Expect(bq.Size()).To(Equal(7))

		batch := bq.DequeueBatch(nil, 3)
		Expect(batch).To(Equal([]int{0, 1, 2}))
		batch = bq.DequeueBatch(batch, 100)
		Expect(batch).To(Equal([]int{3, 4, 5, 6}))
		Expect(bq.IsEmpty()).To(BeTrue())
	})

	It("does not grow the backing array when the queue never fully drains", func() {
		const capacity = 10
		bq := NewBlockingQueue[int](capacity)
		for i := 0; i < capacity/2; i++ {
			Expect(bq.Enqueue(i)).To(Succeed())
		}
		// Keep the queue non-empty: enqueue one, dequeue one, so the head is
		// never reset by a full drain.
		var dst []int
		for i := 0; i < 10_000; i++ {
			Expect(bq.Enqueue(i)).To(Succeed())
			dst = bq.DequeueBatch(dst, 1)
			Expect(dst).To(HaveLen(1))
		}
		bq.mu.Lock()
		backing := cap(bq.items)
		bq.mu.Unlock()
		Expect(backing).To(BeNumerically("<=", 4*capacity))
		Expect(bq.Size()).To(Equal(capacity / 2))
	})

	It("keeps FIFO order across compactions", func() {
		bq := NewBlockingQueue[int](8)
		next, expected := 0, 0
		for i := 0; i < 4; i++ {
			Expect(bq.Enqueue(next)).To(Succeed())
			next++
		}
		var dst []int
		for i := 0; i < 1000; i++ {
			Expect(bq.Enqueue(next)).To(Succeed())
			next++
			dst = bq.DequeueBatch(dst, 1)
			Expect(dst[0]).To(Equal(expected))
			expected++
		}
	})

	It("blocks Enqueue when full until an item is dequeued", func() {
		bq := NewBlockingQueue[int](2)
		Expect(bq.Enqueue(1)).To(Succeed())
		Expect(bq.Enqueue(2)).To(Succeed())

		done := make(chan error, 1)
		go func() { done <- bq.Enqueue(3) }()
		Consistently(done, 100*time.Millisecond).ShouldNot(Receive())

		Expect(bq.DequeueBatch(nil, 1)).To(Equal([]int{1}))
		Eventually(done).Should(Receive(BeNil()))
		Expect(bq.Size()).To(Equal(2))
	})

	It("Stop releases blocked senders and returns the pending items", func() {
		bq := NewBlockingQueue[int](2)
		Expect(bq.Enqueue(1)).To(Succeed())
		Expect(bq.Enqueue(2)).To(Succeed())

		done := make(chan error, 1)
		go func() { done <- bq.Enqueue(3) }()
		Consistently(done, 100*time.Millisecond).ShouldNot(Receive())

		pending := bq.Stop()
		Expect(pending).To(Equal([]int{1, 2}))
		Eventually(done).Should(Receive(MatchError(ErrBlockingQueueStopped)))
		Expect(bq.Enqueue(4)).To(MatchError(ErrBlockingQueueStopped))
		Expect(bq.IsStopped()).To(BeTrue())
	})

	It("Close wakes a blocked DequeueBatch, which returns nil", func() {
		bq := NewBlockingQueue[int](2)
		result := make(chan []int, 1)
		go func() { result <- bq.DequeueBatch(nil, 10) }()
		Consistently(result, 100*time.Millisecond).ShouldNot(Receive())

		bq.Stop()
		bq.Close()
		Eventually(result).Should(Receive(BeNil()))
	})

	It("Close is a no-op when the queue was not stopped", func() {
		bq := NewBlockingQueue[int](2)
		bq.Close()
		Expect(bq.IsStopped()).To(BeFalse())
		Expect(bq.Enqueue(1)).To(Succeed())
	})

	It("delivers every item exactly once with concurrent senders", func() {
		const senders, perSender = 8, 2000
		bq := NewBlockingQueue[int](64)

		var wg sync.WaitGroup
		for s := 0; s < senders; s++ {
			wg.Add(1)
			go func(s int) {
				defer GinkgoRecover()
				defer wg.Done()
				for i := 0; i < perSender; i++ {
					Expect(bq.Enqueue(s*perSender + i)).To(Succeed())
				}
			}(s)
		}

		seen := make(map[int]struct{}, senders*perSender)
		lastPerSender := make([]int, senders)
		for s := range lastPerSender {
			lastPerSender[s] = -1
		}
		var dst []int
		for len(seen) < senders*perSender {
			dst = bq.DequeueBatch(dst, 32)
			Expect(dst).NotTo(BeNil())
			for _, v := range dst {
				Expect(seen).NotTo(HaveKey(v))
				seen[v] = struct{}{}
				// per-sender order must be preserved
				s, i := v/perSender, v%perSender
				Expect(i).To(BeNumerically(">", lastPerSender[s]))
				lastPerSender[s] = i
			}
		}
		wg.Wait()
		Expect(bq.IsEmpty()).To(BeTrue())
	})
})
