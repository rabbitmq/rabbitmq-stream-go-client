package stream

import (
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// These specs use the coordinator to reproduce close-before-registration without
// needing a broker or relying on a network failure arriving at a particular time.
var _ = Describe("Close notification retention", func() {
	It("retains a producer close event registered after the close", func() {
		producer, err := NewCoordinator().NewProducer(nil, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(producer.close(Event{Reason: SocketClosed})).To(Succeed())

		events := producer.NotifyClose()
		Expect(producer.NotifyClose()).To(BeIdenticalTo(events), "each call must return the same channel")

		var event Event
		Eventually(events, time.Second).Should(Receive(&event), "early producer close event was lost")
		Expect(event.Reason).To(Equal(SocketClosed))
		Eventually(events, time.Second).Should(BeClosed())
	})

	It("retains a consumer close event registered after the close", func() {
		consumer, err := NewCoordinator().NewConsumer(nil, NewConsumerOptions(), nil)
		Expect(err).NotTo(HaveOccurred())
		consumer.close(Event{Reason: SocketClosed})

		events := consumer.NotifyClose()
		Expect(consumer.NotifyClose()).To(BeIdenticalTo(events), "each call must return the same channel")

		var event Event
		Eventually(events, time.Second).Should(Receive(&event), "early consumer close event was lost")
		Expect(event.Reason).To(Equal(SocketClosed))
		Eventually(events, time.Second).Should(BeClosed())
	})

	It("gives concurrent producer closes exactly one owner", func() {
		for range 100 {
			producer, err := NewCoordinator().NewProducer(nil, nil)
			Expect(err).NotTo(HaveOccurred())

			const closers = 32
			start := make(chan struct{})
			results := make(chan error, closers)
			for range closers {
				go func() {
					defer GinkgoRecover()
					<-start
					results <- producer.close(Event{Reason: SocketClosed})
				}()
			}
			close(start)

			owners := 0
			for range closers {
				var closeErr error
				Eventually(results, time.Second).Should(Receive(&closeErr), "concurrent producer close blocked")
				if closeErr == nil {
					owners++
				} else {
					Expect(closeErr).To(MatchError(AlreadyClosed))
				}
			}
			Expect(owners).To(Equal(1))

			events := producer.NotifyClose()
			Eventually(events, time.Second).Should(Receive())
			Eventually(events, time.Second).Should(BeClosed())
		}
	})
})

var _ = Describe("Super stream partition notifications", func() {
	// partitionsClosed reports the mutex-guarded shutdown flag for polling.
	partitionsClosed := func(consumer *SuperStreamConsumer) func() bool {
		return func() bool {
			consumer.chSuperStreamPartitionMutex.Lock()
			defer consumer.chSuperStreamPartitionMutex.Unlock()
			return consumer.partitionNotificationsClosed
		}
	}

	It("retains partition events registered after the close", func() {
		consumer := &SuperStreamConsumer{stopPartitionForwarding: make(chan struct{})}
		for _, partition := range []string{"events-0", "events-1"} {
			consumer.notifyPartitionClose(CPartitionClose{Partition: partition, Event: Event{Reason: SocketClosed}})
		}
		Expect(consumer.Close()).To(Succeed())
		Eventually(partitionsClosed(consumer)).WithTimeout(time.Second).WithPolling(time.Millisecond).
			Should(BeTrue())

		events := consumer.NotifyPartitionClose(1)
		Expect(consumer.NotifyPartitionClose(10)).To(BeIdenticalTo(events),
			"each call must return the same channel")

		partitions := make([]string, 0, 2)
		for event := range events {
			partitions = append(partitions, event.Partition)
		}
		Expect(partitions).To(Equal([]string{"events-0", "events-1"}))

		Expect(consumer.Close()).To(Succeed(), "Close must be idempotent")
		Expect(consumer.ConnectPartition("events-0", OffsetSpecification{}.First())).
			To(MatchError(AlreadyClosed))
	})

	It("waits for in-flight partition forwarders on Close", func() {
		consumer := &SuperStreamConsumer{stopPartitionForwarding: make(chan struct{})}
		events := consumer.NotifyPartitionClose(1)

		consumer.partitionCloseWorkers.Add(1)
		started := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer consumer.partitionCloseWorkers.Done()
			close(started)
			consumer.notifyPartitionClose(CPartitionClose{Partition: "events-0"})
		}()

		Eventually(started, time.Second).Should(BeClosed())
		Expect(consumer.Close()).To(Succeed())

		// A slow reader must not lose its event to a timer-based channel close.
		var event CPartitionClose
		Eventually(events, time.Second).Should(Receive(&event), "partition forwarder did not complete")
		Expect(event.Partition).To(Equal("events-0"))
		Eventually(events, time.Second).Should(BeClosed(), "partition notification channel was not closed")
	})

	It("honours the largest requested buffer under concurrent registration", func() {
		consumer := &SuperStreamConsumer{stopPartitionForwarding: make(chan struct{})}
		consumer.partitionCloseWorkers.Add(1)

		var wg sync.WaitGroup
		for range 20 {
			wg.Go(func() {
				defer GinkgoRecover()
				consumer.NotifyPartitionClose(64)
			})
		}
		wg.Go(func() {
			defer GinkgoRecover()
			defer consumer.partitionCloseWorkers.Done()
			consumer.notifyPartitionClose(CPartitionClose{Partition: "events-0"})
		})
		wg.Wait()

		Expect(cap(consumer.NotifyPartitionClose(1))).To(BeNumerically(">=", 64))
		Expect(consumer.Close()).To(Succeed())

		events := make([]CPartitionClose, 0, 1)
		for event := range consumer.NotifyPartitionClose(1) {
			events = append(events, event)
		}
		Expect(events).To(HaveLen(1))
		Expect(events[0].Partition).To(Equal("events-0"))
	})

	It("does not wait for an abandoned reader", func() {
		consumer := &SuperStreamConsumer{stopPartitionForwarding: make(chan struct{})}
		events := consumer.NotifyPartitionClose(1)
		consumer.notifyPartitionClose(CPartitionClose{Partition: "old-event"})

		consumer.partitionCloseWorkers.Add(1)
		go func() {
			defer GinkgoRecover()
			defer consumer.partitionCloseWorkers.Done()
			consumer.notifyPartitionClose(CPartitionClose{Partition: "shutdown-event"})
		}()

		Expect(consumer.Close()).To(Succeed())
		Eventually(partitionsClosed(consumer)).WithTimeout(time.Second).WithPolling(time.Millisecond).
			Should(BeTrue())

		var event CPartitionClose
		Eventually(events, time.Second).Should(Receive(&event))
		Expect(event.Partition).To(Equal("old-event"))
		Eventually(events, time.Second).Should(BeClosed())
	})
})
