package ha

import (
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

// Embedding the real reliable consumer retains its shutdown behavior while
// exposing the exact point at which retry enters backoff, without a broker.
type retryCloseConsumer struct {
	*ReliableSuperStreamConsumer
	entered chan struct{}
}

func (r *retryCloseConsumer) getInfo() string {
	select {
	case r.entered <- struct{}{}:
	default:
	}
	return "close-during-retry"
}

var _ = Describe("Reliable Super Stream Consumer shutdown", func() {
	It("drains terminal partition events", func() {
		r := &ReliableSuperStreamConsumer{
			consumerOptions: stream.NewSuperStreamConsumerOptions(),
			mutexStatus:     &sync.Mutex{},
			status:          StatusOpen,
		}
		events := make(chan stream.CPartitionClose, 1)
		r.handleNotifyClose(events)

		sent := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(sent)
			defer close(events)
			// A terminal event followed by unexpected closes must be drained without
			// starting reconnection against this deliberately absent environment.
			events <- stream.CPartitionClose{Event: stream.Event{Reason: stream.UnSubscribe}}
			for range 4 {
				events <- stream.CPartitionClose{Event: stream.Event{Reason: stream.SocketClosed}}
			}
		}()

		Eventually(sent, time.Second).Should(BeClosed(),
			"HA listener abandoned pending partition events")
		Eventually(r.GetStatus).WithTimeout(time.Second).WithPolling(time.Millisecond).
			Should(Equal(StatusClosed))
	})

	It("interrupts retry backoff on a terminal Close", func() {
		r := &ReliableSuperStreamConsumer{
			consumerOptions: stream.NewSuperStreamConsumerOptions(),
			mutexStatus:     &sync.Mutex{},
			status:          StatusReconnecting,
			stopRetry:       make(chan struct{}),
		}
		r.consumer.Store(&stream.SuperStreamConsumer{})
		observer := &retryCloseConsumer{ReliableSuperStreamConsumer: r, entered: make(chan struct{}, 1)}

		finished := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			err, connected := retry(1, observer, "events-0")
			if connected {
				finished <- nil
			} else {
				finished <- err
			}
		}()

		Eventually(observer.entered, time.Second).Should(Receive(), "retry did not enter backoff")
		Expect(r.Close()).To(Succeed())

		var err error
		Eventually(finished, time.Second).Should(Receive(&err),
			"terminal Close did not stop pending retry")
		Expect(err).To(MatchError(stream.AlreadyClosed))

		Expect(r.GetStatus()).To(Equal(StatusClosed))
		Expect(r.Close()).To(Succeed(), "Close must be idempotent")
	})
})

// retryStopper is a reliable entity whose Close interrupts retry.
type retryStopper interface {
	IReliable
	retryStop() <-chan struct{}
}

// retryCloseObserver wraps any reliable entity, retaining its shutdown
// behavior while exposing the point at which retry enters backoff.
type retryCloseObserver struct {
	retryStopper
	entered chan struct{}
}

func (r *retryCloseObserver) getInfo() string {
	select {
	case r.entered <- struct{}{}:
	default:
	}
	return "close-during-retry"
}

// newReconnectingProducer returns a producer whose connection was dropped:
// its inner producer is already closed and no environment is available.
func newReconnectingProducer() *ReliableProducer {
	dead, err := stream.NewCoordinator().NewProducer(nil, nil)
	Expect(err).NotTo(HaveOccurred())
	Expect(dead.Close()).To(Succeed())
	p := &ReliableProducer{
		producerOptions:    stream.NewProducerOptions(),
		mutex:              &sync.Mutex{},
		mutexStatus:        &sync.Mutex{},
		status:             StatusReconnecting,
		reconnectionSignal: sync.NewCond(&sync.Mutex{}),
		stopRetry:          make(chan struct{}),
	}
	p.producer.Store(dead)
	return p
}

var _ = Describe("Reliable entities Close during reconnection", func() {
	DescribeTable("interrupt retry backoff and never create a new instance",
		func(newReliable func() (retryStopper, func() error)) {
			reliable, closeReliable := newReliable()
			observer := &retryCloseObserver{retryStopper: reliable, entered: make(chan struct{}, 1)}

			finished := make(chan error, 1)
			go func() {
				defer GinkgoRecover()
				err, connected := retry(1, observer, "events")
				if connected {
					finished <- nil
				} else {
					finished <- err
				}
			}()

			Eventually(observer.entered, time.Second).Should(Receive(), "retry did not enter backoff")
			Expect(closeReliable()).To(Succeed(), "Close of a reconnecting entity must succeed")

			var err error
			Eventually(finished, time.Second).Should(Receive(&err),
				"Close did not stop pending retry")
			Expect(err).To(MatchError(stream.AlreadyClosed))
			Expect(reliable.GetStatus()).To(Equal(StatusClosed))

			// The environment is deliberately absent: creating an instance would panic.
			Expect(reliable.getNewInstance("events")()).To(MatchError(stream.AlreadyClosed))
			Expect(closeReliable()).To(Succeed(), "Close must be idempotent")
			Expect(reliable.GetStatus()).To(Equal(StatusClosed))
		},
		Entry("ReliableProducer", func() (retryStopper, func() error) {
			p := newReconnectingProducer()
			return p, p.Close
		}),
		Entry("ReliableConsumer", func() (retryStopper, func() error) {
			c := &ReliableConsumer{
				consumerOptions: stream.NewConsumerOptions(),
				mutexStatus:     &sync.Mutex{},
				mutexConnection: &sync.Mutex{},
				status:          StatusReconnecting,
				stopRetry:       make(chan struct{}),
			}
			return c, c.Close
		}),
		Entry("ReliableSuperStreamProducer", func() (retryStopper, func() error) {
			r := &ReliableSuperStreamProducer{
				producerOptions:    stream.NewSuperStreamProducerOptions(nil),
				mutex:              &sync.Mutex{},
				mutexStatus:        &sync.Mutex{},
				status:             StatusReconnecting,
				reconnectionSignal: sync.NewCond(&sync.Mutex{}),
				stopRetry:          make(chan struct{}),
			}
			r.producer.Store(&stream.SuperStreamProducer{})
			return r, r.Close
		}),
	)

	It("interrupts the reliable producer wait before retry", func() {
		p := newReconnectingProducer()
		p.status = StatusOpen

		// Wait for the reconnection signal the handler sends when it finishes.
		// The waiter holds the lock until Wait releases it, so the signal is not lost.
		waiting := make(chan struct{})
		signalled := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			p.reconnectionSignal.L.Lock()
			close(waiting)
			p.reconnectionSignal.Wait()
			p.reconnectionSignal.L.Unlock()
			close(signalled)
		}()
		Eventually(waiting, time.Second).Should(BeClosed())

		events := make(chan stream.Event, 1)
		events <- stream.Event{Reason: stream.SocketClosed}
		p.handleNotifyClose(events)
		Eventually(p.GetStatus).WithTimeout(time.Second).WithPolling(time.Millisecond).
			Should(Equal(StatusReconnecting))

		Expect(p.Close()).To(Succeed())
		// The wait before retry lasts at least three seconds.
		Eventually(signalled, time.Second).Should(BeClosed(), "Close did not stop the pending reconnection")
		Expect(p.GetStatus()).To(Equal(StatusClosed))
		Expect(p.Close()).To(Succeed(), "Close must be idempotent")
	})

	It("does not reconnect a closed reliable consumer", func() {
		c := &ReliableConsumer{
			consumerOptions: stream.NewConsumerOptions(),
			mutexStatus:     &sync.Mutex{},
			mutexConnection: &sync.Mutex{},
			status:          StatusOpen,
			stopRetry:       make(chan struct{}),
		}
		Expect(c.Close()).To(Succeed())

		// A late close event must not move a closed consumer to reconnecting.
		events := make(chan stream.Event, 1)
		events <- stream.Event{Reason: stream.SocketClosed}
		c.handleNotifyClose(events)
		Consistently(c.GetStatus).WithTimeout(200 * time.Millisecond).WithPolling(time.Millisecond).
			Should(Equal(StatusClosed))
	})

	It("drains terminal super stream producer partition events", func() {
		r := &ReliableSuperStreamProducer{
			producerOptions:    stream.NewSuperStreamProducerOptions(nil),
			mutex:              &sync.Mutex{},
			mutexStatus:        &sync.Mutex{},
			status:             StatusOpen,
			reconnectionSignal: sync.NewCond(&sync.Mutex{}),
			stopRetry:          make(chan struct{}),
		}
		events := make(chan stream.PPartitionClose, 1)
		r.handleNotifyClose(events)

		sent := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(sent)
			defer close(events)
			// A terminal event followed by unexpected closes must be drained without
			// starting reconnection against this deliberately absent environment.
			events <- stream.PPartitionClose{Event: stream.Event{Reason: stream.DeletePublisher}}
			for range 4 {
				events <- stream.PPartitionClose{Event: stream.Event{Reason: stream.SocketClosed}}
			}
		}()

		Eventually(sent, time.Second).Should(BeClosed(),
			"HA listener abandoned pending partition events")
		Eventually(r.GetStatus).WithTimeout(time.Second).WithPolling(time.Millisecond).
			Should(Equal(StatusClosed))
	})
})
