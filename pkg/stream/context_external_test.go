package stream_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

var _ = Describe("Environment lifetime context", func() {
	It("rejects an invalid lifetime context", func() {
		_, err := stream.NewEnvironmentWithContext(nil, nil) //nolint:staticcheck // Verify the public constructor rejects invalid contexts.
		Expect(err).To(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = stream.NewEnvironmentWithContext(ctx, nil)
		Expect(err).To(MatchError(context.Canceled))
	})

	It("aborts a stalled handshake on the context deadline", func() {
		listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = listener.Close() })

		peerDone := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(peerDone)
			peer, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			defer func() { _ = peer.Close() }()
			_, _ = io.Copy(io.Discard, peer)
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		DeferCleanup(cancel)

		started := time.Now()
		_, err = stream.NewEnvironmentWithContext(ctx, stream.NewEnvironmentOptions().
			SetUri("rabbitmq-stream://guest:guest@"+listener.Addr().String()+"/").
			SetRPCTimeout(time.Hour))
		Expect(err).To(MatchError(context.DeadlineExceeded))
		// The 100ms deadline must win over the one hour RPC timeout.
		Expect(time.Since(started)).To(BeNumerically("<", 2*time.Second))

		Eventually(peerDone, time.Second).Should(BeClosed(),
			"deadline did not release the handshake socket")
	})

	// The upstream suite provides RabbitMQ on the default local listener.
	DescribeTable("tears the environment down",
		func(cancelFirst bool) {
			cleanup, err := stream.NewEnvironment(nil)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = cleanup.Close() })

			name := fmt.Sprintf("context-lifetime-%d", time.Now().UnixNano())
			Expect(cleanup.DeclareStream(name, nil)).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = cleanup.DeleteStream(name) })

			options := stream.NewEnvironmentOptions().SetRPCTimeout(time.Hour)
			ctx, cancel := context.WithCancel(context.Background())
			DeferCleanup(cancel)

			env, err := stream.NewEnvironmentWithContext(ctx, options)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() { _ = env.Close() })

			producer, err := env.NewProducer(name, nil)
			Expect(err).NotTo(HaveOccurred())
			confirmations := producer.NotifyPublishConfirmation()
			closeEvents := producer.NotifyClose()
			Expect(producer.Send(amqp.NewMessage([]byte("before cancellation")))).NotTo(HaveOccurred())

			var batch []*stream.ConfirmationStatus
			Eventually(confirmations, 3*time.Second).Should(Receive(&batch), "publication not confirmed")
			Expect(batch).To(HaveLen(1))
			Expect(batch[0].IsConfirmed()).To(BeTrue())

			if cancelFirst {
				cancel()
			}
			done := make(chan error, 1)
			go func() {
				defer GinkgoRecover()
				done <- env.Close()
			}()

			var closeErr error
			Eventually(done, time.Second).Should(Receive(&closeErr), "Close ignored canceled environment")
			Expect(closeErr).NotTo(HaveOccurred())

			Eventually(closeEvents, time.Second).Should(Receive(), "producer socket remained active")
			Expect(env.IsClosed()).To(BeTrue())

			_, err = env.QueryPartitions(name)
			Expect(err).To(MatchError(context.Canceled))

			// Reusing the caller's options starts an independent lifetime.
			next, err := stream.NewEnvironmentWithContext(context.Background(), options)
			Expect(err).NotTo(HaveOccurred())
			Expect(next.Close()).To(Succeed())
		},

		Entry("when the lifetime context is canceled", true),
		Entry("when Close is called directly", false),
	)
})
