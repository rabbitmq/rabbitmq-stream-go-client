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
