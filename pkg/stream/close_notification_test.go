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

var _ = Describe("Super stream producer shutdown", func() {
	// connectPartition registers a broker-free partition producer exactly as
	// ConnectPartition does once NewProducer has succeeded.
	connectPartition := func(superProducer *SuperStreamProducer, partition string) *Producer {
		producer, err := NewCoordinator().NewProducer(nil, nil)
		Expect(err).NotTo(HaveOccurred())
		superProducer.mutex.Lock()
		defer superProducer.mutex.Unlock()
		superProducer.partitions = append(superProducer.partitions, partition)
		superProducer.activeProducers = append(superProducer.activeProducers, producer)
		superProducer.startPartitionForwarders(partition, producer)
		return producer
	}

	// notificationsClosed reports whether Close released both registered channels.
	notificationsClosed := func(superProducer *SuperStreamProducer) func() bool {
		return func() bool {
			superProducer.chSuperStreamPartitionMutex.Lock()
			defer superProducer.chSuperStreamPartitionMutex.Unlock()
			return superProducer.chNotifyPublishConfirmation == nil && superProducer.chSuperStreamPartitionClose == nil
		}
	}

	// drainConfirmations reads every confirmation until the channel is closed and
	// returns the publishing ids, after sleeping delay before each read.
	drainConfirmations := func(confirmations <-chan PartitionPublishConfirm, delay time.Duration) <-chan []int64 {
		result := make(chan []int64, 1)
		go func() {
			ids := make([]int64, 0)
			for {
				time.Sleep(delay)
				confirm, ok := <-confirmations
				if !ok {
					break
				}
				for _, status := range confirm.ConfirmationStatus {
					ids = append(ids, status.GetPublishingId())
				}
			}
			result <- ids
		}()
		return result
	}

	// drainPartitionEvents reads every partition close event until the channel is closed.
	drainPartitionEvents := func(events <-chan PPartitionClose) <-chan []string {
		result := make(chan []string, 1)
		go func() {
			partitions := make([]string, 0)
			for event := range events {
				partitions = append(partitions, event.Partition)
			}
			result <- partitions
		}()
		return result
	}

	confirmation := func(publishingId int64) []*ConfirmationStatus {
		return []*ConfirmationStatus{{publishingId: publishingId}}
	}

	It("closes every partition, even after an already closed one, and cannot reconnect", func() {
		// A partition producer closed by a dropped connection must not abort Close.
		dead, err := NewCoordinator().NewProducer(nil, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(dead.Close()).To(Succeed())
		live, err := NewCoordinator().NewProducer(nil, nil)
		Expect(err).NotTo(HaveOccurred())

		superProducer := &SuperStreamProducer{
			activeProducers: []*Producer{dead, live},
			partitions:      []string{"events-0", "events-1"},
		}
		confirmations := superProducer.NotifyPublishConfirmation(1)

		Expect(superProducer.Close()).To(Succeed())
		Expect(live.Close()).To(MatchError(AlreadyClosed), "remaining partitions must be closed")
		Eventually(confirmations, time.Second).Should(BeClosed(),
			"publish confirmation channel was not released")

		Expect(superProducer.ConnectPartition("events-0")).To(MatchError(AlreadyClosed))
		Expect(superProducer.Close()).To(Succeed(), "Close must be idempotent")
	})

	It("returns from Close with unread notifications and delivers all of them once drained", func() {
		superProducer := &SuperStreamProducer{}
		confirmations := superProducer.NotifyPublishConfirmation(1)
		partitionEvents := superProducer.NotifyPartitionClose(1)
		producer := connectPartition(superProducer, "events-0")
		connectPartition(superProducer, "events-1")

		// Nobody reads yet: the first confirmation fills the buffer, the forwarder
		// blocks on the second and the third waits in the partition producer.
		for publishingId := range int64(3) {
			producer.sendConfirmationStatus(confirmation(publishingId))
		}

		closed := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			closed <- superProducer.Close()
		}()
		var err error
		Eventually(closed, time.Second).Should(Receive(&err), "Close waited for the readers")
		Expect(err).NotTo(HaveOccurred())
		Consistently(notificationsClosed(superProducer)).WithTimeout(200*time.Millisecond).WithPolling(time.Millisecond).
			Should(BeFalse(), "notification channels closed before pending notifications were delivered")

		confirmed := drainConfirmations(confirmations, 0)
		closedPartitions := drainPartitionEvents(partitionEvents)
		var ids []int64
		Eventually(confirmed, time.Second).Should(Receive(&ids), "confirmation channel was not closed")
		Expect(ids).To(Equal([]int64{0, 1, 2}), "a confirmation was lost")
		var partitions []string
		Eventually(closedPartitions, time.Second).Should(Receive(&partitions), "partition close channel was not closed")
		Expect(partitions).To(ConsistOf("events-0", "events-1"), "a partition close event was lost")
		Expect(notificationsClosed(superProducer)()).To(BeTrue())
	})

	It("delivers every confirmation to a slow reader of an unbuffered channel before closing it", func() {
		superProducer := &SuperStreamProducer{}
		confirmations := superProducer.NotifyPublishConfirmation(0)
		producer := connectPartition(superProducer, "events-0")
		// Messages never sent are reported as unconfirmed by Close itself.
		for _, publishingId := range []int64{100, 101} {
			Expect(producer.pendingSequencesQueue.Enqueue(&messageSequence{publishingId: publishingId})).To(Succeed())
		}

		confirmed := drainConfirmations(confirmations, 20*time.Millisecond)
		sent := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(sent)
			for publishingId := range int64(3) {
				producer.sendConfirmationStatus(confirmation(publishingId))
			}
		}()
		// The confirmations are still being delivered to the slow reader.
		Eventually(sent, time.Second).Should(BeClosed())

		closed := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			closed <- superProducer.Close()
		}()
		var err error
		Eventually(closed, 2*time.Second).Should(Receive(&err))
		Expect(err).NotTo(HaveOccurred())

		var ids []int64
		Eventually(confirmed, 2*time.Second).Should(Receive(&ids), "confirmation channel was not closed")
		Expect(ids).To(Equal([]int64{0, 1, 2, 100, 101}), "a confirmation was lost")
	})

	It("finishes partition forwarders before closing the notification channels", func() {
		// A send on a closed channel panics the process; -race also reports
		// unsynchronized access to the channel fields.
		for range 50 {
			superProducer := &SuperStreamProducer{}
			confirmations := superProducer.NotifyPublishConfirmation(1)
			partitionEvents := superProducer.NotifyPartitionClose(1)
			producers := []*Producer{
				connectPartition(superProducer, "events-0"),
				connectPartition(superProducer, "events-1"),
				connectPartition(superProducer, "events-2"),
			}

			confirmed := drainConfirmations(confirmations, 0)
			closedPartitions := drainPartitionEvents(partitionEvents)

			// Confirmations race with Close.
			var senders sync.WaitGroup
			for _, producer := range producers {
				senders.Go(func() {
					for publishingId := range int64(20) {
						producer.sendConfirmationStatus(confirmation(publishingId))
					}
				})
			}
			Expect(superProducer.Close()).To(Succeed())
			senders.Wait()

			Eventually(confirmed, time.Second).Should(Receive(), "confirmation channel was not closed")
			var partitions []string
			Eventually(closedPartitions, time.Second).Should(Receive(&partitions), "partition close channel was not closed")
			Expect(partitions).To(ConsistOf("events-0", "events-1", "events-2"), "a partition close event was lost")
		}
	})

	It("closes the notification channels promptly for an active reader", func() {
		superProducer := &SuperStreamProducer{}
		confirmed := drainConfirmations(superProducer.NotifyPublishConfirmation(1), 0)
		closedPartitions := drainPartitionEvents(superProducer.NotifyPartitionClose(1))
		connectPartition(superProducer, "events-0")

		Expect(superProducer.Close()).To(Succeed())
		// The previous implementation closed them after a two second timer.
		Eventually(confirmed, 500*time.Millisecond).Should(Receive())
		var partitions []string
		Eventually(closedPartitions, 500*time.Millisecond).Should(Receive(&partitions))
		Expect(partitions).To(Equal([]string{"events-0"}))
		Expect(notificationsClosed(superProducer)()).To(BeTrue())
	})
})
