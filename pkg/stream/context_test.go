package stream

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type observedContextConn struct {
	net.Conn
	writes chan struct{}
	once   atomic.Bool
}

func (c *observedContextConn) Write(data []byte) (int, error) {
	if c.once.CompareAndSwap(false, true) {
		close(c.writes)
	}
	return c.Conn.Write(data)
}

// metadataCancelConn completes the first metadata response without a leader,
// then cancels while the second response is outstanding.
type metadataCancelConn struct {
	net.Conn
	client   *Client
	cancel   context.CancelFunc
	requests atomic.Int32
	leader   *Broker
}

func (c *metadataCancelConn) Write(data []byte) (int, error) {
	if c.requests.Add(1) > 1 {
		c.cancel()
		return len(data), nil
	}
	response, err := c.client.coordinator.GetResponseById(binary.BigEndian.Uint32(data[8:12]))
	if err != nil {
		return 0, err
	}
	metadata := StreamsMetadata{}.New()
	metadata.Add("context-metadata", responseCodeOk, c.leader, nil)
	response.code <- Code{id: responseCodeOk}
	response.data <- metadata
	return len(data), nil
}

var _ = Describe("Connection context cancellation", func() {
	It("aborts a black-holed dial", func() {
		ctx, cancel := context.WithCancel(context.Background())
		DeferCleanup(cancel)

		entered := make(chan struct{})
		options := NewEnvironmentOptions().SetRPCTimeout(time.Hour)
		options.TCPParameters.dialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}

		done := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			_, err := NewEnvironmentWithContext(ctx, options)
			done <- err
		}()

		Eventually(entered, time.Second).Should(BeClosed(), "dial did not start")
		cancel()

		var err error
		Eventually(done, time.Second).Should(Receive(&err), "dial ignored cancellation")
		Expect(err).To(MatchError(context.Canceled))
		Expect(options.TCPParameters.connectionContext).To(BeNil(), "constructor mutated caller options")
	})

	DescribeTable("interrupts a stalled handshake",
		func(uri string, drainPeer bool) {
			client, server := net.Pipe()
			DeferCleanup(func() { _ = server.Close() })

			ctx, cancel := context.WithCancel(context.Background())
			DeferCleanup(cancel)

			writes := make(chan struct{})
			readerDone := make(chan struct{})
			if drainPeer {
				go func() {
					defer GinkgoRecover()
					defer close(readerDone)
					_, _ = io.Copy(io.Discard, server)
				}()
			} else {
				close(readerDone)
			}

			options := NewEnvironmentOptions().SetUri(uri).SetRPCTimeout(time.Hour)
			options.TCPParameters.dialContext = func(context.Context, string, string) (net.Conn, error) {
				return &observedContextConn{Conn: client, writes: writes}, nil
			}

			done := make(chan error, 1)
			go func() {
				defer GinkgoRecover()
				_, err := NewEnvironmentWithContext(ctx, options)
				done <- err
			}()

			// The handshake must reach real I/O before cancellation is meaningful,
			// so surface an early constructor failure rather than a bare timeout.
			var failedEarly bool
			var earlyErr error
			Eventually(func() bool {
				select {
				case <-writes:
					return true
				case earlyErr = <-done:
					failedEarly = true
					return true
				default:
					return false
				}
			}).WithTimeout(time.Second).WithPolling(10*time.Millisecond).
				Should(BeTrue(), "handshake never started")
			Expect(failedEarly).To(BeFalse(), "handshake failed before I/O: %v", earlyErr)

			cancel()

			var err error
			Eventually(done, time.Second).Should(Receive(&err), "handshake ignored cancellation")
			Expect(err).To(MatchError(context.Canceled))

			Eventually(readerDone, time.Second).Should(BeClosed(), "socket reader leaked")

			// Writing into the peer proves the client end really was closed.
			_, err = server.Write([]byte("after cancellation"))
			Expect(err).To(HaveOccurred())
		},

		Entry("blocked on the initial write", "rabbitmq-stream://guest:guest@localhost:5552/", false),
		Entry("awaiting a response", "rabbitmq-stream://guest:guest@localhost:5552/", true),
		Entry("inside the TLS handshake", "rabbitmq-stream+tls://guest:guest@localhost:5551/", true),
	)

	DescribeTable("stops a blocked protocol wait",
		func(wait func(*Client, *Response) error) {
			ctx, cancel := context.WithCancel(context.Background())
			client := &Client{
				coordinator:       NewCoordinator(),
				tcpParameters:     &TCPParameters{connectionContext: ctx},
				socketCallTimeout: time.Hour,
			}
			response := newResponse("context-test")

			done := make(chan error, 1)
			go func() {
				defer GinkgoRecover()
				done <- wait(client, response)
			}()

			cancel()

			var err error
			Eventually(done, time.Second).Should(Receive(&err), "protocol wait ignored cancellation")
			Expect(err).To(MatchError(context.Canceled))
		},

		Entry("waitCode", func(client *Client, response *Response) error {
			return client.waitCode(response).Err
		}),
		Entry("waitData", func(client *Client, response *Response) error {
			_, err := client.waitData(response)
			return err
		}),
	)
})

var _ = Describe("Locator reconnect cancellation", func() {
	var (
		cancel  context.CancelFunc
		entered chan struct{}
		dials   atomic.Int32
		env     *Environment
		done    chan error
	)

	BeforeEach(func() {
		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		DeferCleanup(func() { cancel() })

		entered = make(chan struct{})
		dials = atomic.Int32{}

		options := NewEnvironmentOptions()
		options.ConnectionParameters = []*Broker{newBrokerDefault()}
		options.TCPParameters.connectionContext = ctx
		options.TCPParameters.dialContext = func(context.Context, string, string) (net.Conn, error) {
			if dials.Add(1) == 1 {
				close(entered)
			}
			return nil, errors.New("injected unavailable broker")
		}

		env = &Environment{
			options:   options,
			locator:   newLocator(nil),
			cancel:    cancel,
			closeDone: make(chan struct{}),
			producers: newProducersEnvironment(1, nil),
			consumers: newConsumersEnvironment(1, nil),
		}
		DeferCleanup(func() { _ = env.Close() })

		done = make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			done <- env.maybeReconnectLocator()
		}()

		Eventually(entered, time.Second).Should(BeClosed(), "reconnect never started")
	})

	It("interrupts the backoff when the lifetime context is canceled", func() {
		cancel()

		var err error
		Eventually(done, time.Second).Should(Receive(&err), "reconnect delay ignored cancellation")
		Expect(err).To(MatchError(context.Canceled))

		// A single dial proves the backoff was aborted rather than retried.
		Expect(dials.Load()).To(BeEquivalentTo(1))
	})

	It("interrupts the backoff when Close is called, without blocking Close", func() {
		closed := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			closed <- env.Close()
		}()

		var err error
		Eventually(done, time.Second).Should(Receive(&err), "reconnect delay ignored cancellation")
		Expect(err).To(MatchError(context.Canceled))

		var closeErr error
		Eventually(closed, time.Second).Should(Receive(&closeErr), "Close blocked behind reconnect")
		Expect(closeErr).NotTo(HaveOccurred())

		Expect(dials.Load()).To(BeEquivalentTo(1))
	})
})

var _ = Describe("Metadata lookup cancellation", func() {
	It("stops the leader retry loop", func() {
		ctx, cancel := context.WithCancel(context.Background())
		DeferCleanup(cancel)

		client := newClient(connectionParameters{
			tcpParameters: &TCPParameters{connectionContext: ctx},
			rpcTimeout:    time.Hour,
		})
		local, peer := net.Pipe()
		DeferCleanup(func() { _ = local.Close(); _ = peer.Close() })

		connection := &metadataCancelConn{Conn: local, client: client, cancel: cancel}
		client.setSocketConnection(connection)
		client.socket.setOpen()
		DeferCleanup(client.coordinator.Close)

		env := &Environment{
			options: &EnvironmentOptions{TCPParameters: client.tcpParameters},
			locator: newLocator(client),
		}

		done := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			_, err := env.StreamMetaData("context-metadata")
			done <- err
		}()

		var err error
		Eventually(done, time.Second).Should(Receive(&err), "metadata retry ignored cancellation")
		Expect(err).To(MatchError(context.Canceled))
		Expect(connection.requests.Load()).To(BeEquivalentTo(2))
	})

	It("stops an advertised-host DNS lookup", func() {
		ctx, cancel := context.WithCancel(context.Background())
		DeferCleanup(cancel)

		entered := make(chan struct{})
		parameters := &TCPParameters{
			connectionContext: ctx,
			lookupIPAddr: func(ctx context.Context, _ string) ([]net.IPAddr, error) {
				close(entered)
				<-ctx.Done()
				// The injected error only reads like a cancellation; the
				// context.Canceled identity must come from production code.
				return nil, &net.DNSError{Err: ctx.Err().Error(), IsTimeout: true}
			},
		}

		client := newClient(connectionParameters{tcpParameters: parameters, rpcTimeout: time.Hour})
		local, peer := net.Pipe()
		DeferCleanup(func() { _ = local.Close(); _ = peer.Close() })

		client.setSocketConnection(&metadataCancelConn{
			Conn:   local,
			client: client,
			cancel: cancel,
			leader: &Broker{Host: "advertised.invalid", Port: "5552"},
		})
		client.socket.setOpen()
		DeferCleanup(client.coordinator.Close)

		done := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			_, err := client.BrokerLeader("context-metadata")
			done <- err
		}()

		Eventually(entered, time.Second).Should(BeClosed(), "advertised DNS lookup did not start")
		cancel()

		var err error
		Eventually(done, time.Second).Should(Receive(&err),
			"advertised DNS lookup ignored lifetime cancellation")
		Expect(err).To(MatchError(context.Canceled))
	})
})

var _ = Describe("Abandoned response discarding", func() {
	var (
		cancel   context.CancelFunc
		client   *Client
		peer     net.Conn
		response *Response
	)

	BeforeEach(func() {
		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		DeferCleanup(func() { cancel() })

		client = newClient(connectionParameters{
			tcpParameters: &TCPParameters{connectionContext: ctx},
			rpcTimeout:    time.Hour,
		})

		var local net.Conn
		local, peer = net.Pipe()
		DeferCleanup(func() { _ = local.Close(); _ = peer.Close() })

		client.setSocketConnection(local)
		DeferCleanup(client.coordinator.Close)

		response = client.coordinator.NewResponse(commandMetadata)
	})

	// Every path must clear the coordinator and leave the response channels open,
	// so a reader that already captured them cannot panic on a late send.
	expectDiscarded := func() {
		Expect(client.coordinator.responses).To(BeEmpty())
		response.code <- Code{id: responseCodeOk}
		response.data <- "late response"
	}

	It("discards a response abandoned by a canceled write", func() {
		cancel()
		Expect(client.handleWrite([]byte("request"), response).Err).To(MatchError(context.Canceled))
		expectDiscarded()
	})

	It("discards a response abandoned by a failed write", func() {
		Expect(peer.Close()).To(Succeed())
		Expect(client.handleWrite([]byte("request"), response).Err).To(HaveOccurred())
		expectDiscarded()
	})

	It("discards a response abandoned while awaiting data", func() {
		cancel()
		_, err := client.waitData(response)
		Expect(err).To(MatchError(context.Canceled))
		expectDiscarded()
	})

	It("discards a response abandoned during tune negotiation", func() {
		client.coordinator.discardResponse(response)
		cancel()
		Expect(client.sendSaslAuthenticate("PLAIN", nil)).To(MatchError(context.Canceled))
		expectDiscarded()
	})
})
