package stream

import (
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel"
)

// newLocatorTestEnvironment builds an environment whose locator has never
// connected and whose only broker is addr. backoff replaces the retry delay.
func newLocatorTestEnvironment(addr string, rpcTimeout time.Duration, backoff func(attempt int) time.Duration) *Environment {
	metrics, err := newStreamMetrics(otel.GetMeterProvider())
	Expect(err).NotTo(HaveOccurred())
	host, port, err := net.SplitHostPort(addr)
	Expect(err).NotTo(HaveOccurred())
	broker := newBrokerDefault()
	broker.Host = host
	broker.Port = port
	options := NewEnvironmentOptions()
	options.ConnectionParameters = []*Broker{broker}
	options.RPCTimeout = rpcTimeout
	env := &Environment{
		options:   options,
		producers: newProducersEnvironment(1, metrics),
		consumers: newConsumersEnvironment(1, metrics),
		locator:   newLocator(nil),
		metrics:   metrics,
	}
	env.locator.backoff = backoff
	return env
}

var _ = Describe("Locator reconnection", func() {
	It("Close interrupts the reconnect backoff without blocking", func() {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		addr := l.Addr().String()
		// nothing listens any more: every locator dial is refused
		Expect(l.Close()).To(Succeed())

		var failures atomic.Int32
		env := newLocatorTestEnvironment(addr, time.Second, func(int) time.Duration {
			failures.Add(1)
			return time.Hour
		})

		reconnect := make(chan error, 1)
		go func() { reconnect <- env.maybeReconnectLocator() }()
		Eventually(failures.Load, time.Second).Should(BeNumerically(">=", 1), "the locator never failed a dial")

		closed := make(chan error, 1)
		go func() { closed <- env.Close() }()
		Eventually(closed, time.Second).Should(Receive(BeNil()), "Close blocked on the reconnect")
		Eventually(reconnect, time.Second).Should(Receive(MatchError(AlreadyClosed)),
			"the reconnect kept retrying after Close")
		Expect(env.IsClosed()).To(BeTrue())
	})

	It("closes every failed locator connection attempt", func() {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(l.Close)

		// The fake broker accepts and reads, but never answers the handshake.
		// Each accepted connection reports its EOF, i.e. the client closed it.
		accepted := make(chan chan struct{}, 64)
		go func() {
			for {
				conn, err := l.Accept()
				if err != nil {
					return
				}
				eof := make(chan struct{})
				go func() {
					defer close(eof)
					_, _ = io.Copy(io.Discard, conn)
					_ = conn.Close()
				}()
				accepted <- eof
			}
		}()

		env := newLocatorTestEnvironment(l.Addr().String(), 50*time.Millisecond, func(int) time.Duration {
			return 10 * time.Millisecond
		})
		reconnect := make(chan error, 1)
		go func() { reconnect <- env.maybeReconnectLocator() }()

		var first, second chan struct{}
		Eventually(accepted, 2*time.Second).Should(Receive(&first))
		Eventually(accepted, 2*time.Second).Should(Receive(&second))
		Eventually(first, time.Second).Should(BeClosed(), "a failed attempt kept its connection open")

		Expect(env.Close()).To(Succeed())
		Eventually(reconnect, 2*time.Second).Should(Receive(MatchError(AlreadyClosed)))
		Eventually(second, time.Second).Should(BeClosed())
		for {
			select {
			case eof := <-accepted:
				Eventually(eof, time.Second).Should(BeClosed())
			default:
				return
			}
		}
	})

	It("fails fast after Close instead of dialing a new locator", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		exists, err := env.StreamExists(streamName)
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(BeFalse())
		Expect(env.locator.client.Load().socket.isOpen()).To(BeTrue())

		Expect(env.Close()).To(Succeed())
		Expect(env.locator.client.Load().socket.isOpen()).To(BeFalse())

		Expect(env.DeclareStream(streamName, nil)).To(MatchError(AlreadyClosed))
		_, err = env.StreamMetaData(streamName)
		Expect(err).To(MatchError(AlreadyClosed))
		_, err = env.NewProducer(streamName, nil)
		Expect(err).To(MatchError(AlreadyClosed))
		Expect(env.locator.client.Load().socket.isOpen()).To(BeFalse(), "an operation re-dialed the locator")
	})

	It("does not race when Close runs during a locator reconnect", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		// NewEnvironment leaves the locator closed: the next operation reconnects.
		Expect(env.locator.client.Load().socket.isOpen()).To(BeFalse())

		done := make(chan error, 1)
		go func() {
			_, err := env.StreamExists(uuid.New().String())
			_ = env.IsClosed()
			done <- err
		}()
		Expect(env.Close()).To(Succeed())
		Expect(env.IsClosed()).To(BeTrue())

		var opErr error
		Eventually(done, 5*time.Second).Should(Receive(&opErr))
		if opErr != nil {
			Expect(opErr).To(MatchError(AlreadyClosed))
		}
		Expect(env.locator.client.Load().socket.isOpen()).To(BeFalse(), "the locator client outlived Close")
	})
})
