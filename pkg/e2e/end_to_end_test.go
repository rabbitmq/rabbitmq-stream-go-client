//go:build rabbitmq.stream.e2e

package e2e_test

import (
	"fmt"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/common"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gmeasure"
	"golang.org/x/exp/slog"
	"sync"
	"time"
)

var e2eLogger = slog.New(slog.NewTextHandler(GinkgoWriter))

var _ = Describe("E2E", Serial, Label("e2e"), func() {
	const (
		stream = "e2e-stream-test"
		// 100 byte message
		messageBody = "Rabbitmq-is-awesomeRabbitmq-is-awesomeRabbitmq-is-awesomeRabbitmq-is-awesomeRabbitmq-is-awesome!!!!!"
	)

	It("connects, creates, publishes, deletes and closes", Label("measurement"), func(ctx SpecContext) {
		itCtx := raw.NewContextWithLogger(ctx, *e2eLogger)
		streamClientConfiguration, err := raw.NewClientConfiguration("rabbitmq-stream://guest:guest@localhost/%2F")
		Expect(err).ToNot(HaveOccurred())

		By("dialing the server")
		streamClient, err := raw.DialConfig(itCtx, streamClientConfiguration)
		Expect(err).ToNot(HaveOccurred())

		// Ensure we don't leak open connection on test failures
		DeferCleanup(func(ctx SpecContext) error {
			if streamClient.IsOpen() {
				_ = streamClient.DeleteStream(ctx, stream)
				return streamClient.Close(ctx)
			}
			return nil
		})

		Expect(streamClient.IsOpen()).To(BeTrue(), "expected stream client to be open")

		throughputExp := gmeasure.NewExperiment("100-byte message throughput")
		AddReportEntry(throughputExp.Name, throughputExp)
		stopWatch := throughputExp.NewStopwatch()

		By("creating a stream")
		Expect(streamClient.DeclareStream(itCtx, stream, constants.StreamConfiguration{})).To(Succeed())
		stopWatch.Record("DeclareStream").Reset()

		By("declaring a publisher")
		const publisherId = 1
		Expect(
			streamClient.DeclarePublisher(itCtx, publisherId, "e2e-publisher", stream),
		).To(Succeed())
		stopWatch.Record("DeclarePublisher").Reset()

		By("receiving confirmations")
		c := streamClient.NotifyPublish(make(chan *raw.PublishConfirm, 100))
		var wg sync.WaitGroup
		wg.Add(1)

		const numMessages = 100_000
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			confirmStopwatch := throughputExp.NewStopwatch()
			defer confirmStopwatch.Record("publish confirmation")

			confirmedIds := make(map[uint64]struct{}, numMessages)
			for {
				select {
				case <-ctx.Done():
					Fail(fmt.Sprintf("context timed out: expected to receive 100_000 confirmations: received %d", len(confirmedIds)))
				case confirm, ok := <-c:
					if !ok {
						return
					}
					ids := confirm.PublishingIds()
					for i := 0; i < len(ids); i++ {
						confirmedIds[ids[i]] = struct{}{}
					}

					if len(confirmedIds) == numMessages {
						return
					}
				}
			}
		}()

		By("sending messages")
		for i := uint64(0); i < numMessages; i++ {
			messageContainer := raw.NewPublishingMessage(i, &plainTextMessage{messageBody})
			Expect(
				streamClient.Send(itCtx, publisherId, wrap[common.PublishingMessager](messageContainer)),
			).To(Succeed())
		}
		stopWatch.Record("Send").Reset()

		wg.Wait()

		By("deleting the publisher")
		Expect(streamClient.DeletePublisher(ctx, publisherId)).To(Succeed())
		stopWatch.Record("DeletePublisher").Reset()

		By("deleting a stream")
		Expect(streamClient.DeleteStream(itCtx, stream)).To(Succeed())
		stopWatch.Record("DeleteStream").Reset()

		By("closing the connection")
		Expect(streamClient.Close(itCtx)).To(Succeed())

	}, SpecTimeout(120*time.Second))

	// Send and Recveive Messages, assert messages received are valid.
	It("sends, and receives messages", Label("behaviour"), func(ctx SpecContext) {
		h := slog.HandlerOptions{Level: slog.LevelDebug}.NewTextHandler(GinkgoWriter)
		debugLogger := slog.New(h)
		itCtx := raw.NewContextWithLogger(ctx, *debugLogger)
		streamClientConfiguration, err := raw.NewClientConfiguration("rabbitmq-stream://guest:guest@localhost/%2F")
		Expect(err).ToNot(HaveOccurred())

		By("preparing the environment")
		streamClient, err := raw.DialConfig(itCtx, streamClientConfiguration)
		Expect(err).ToNot(HaveOccurred())

		const stream = "e2e-consume-test"
		// Ensure we don't leak open connection on test failures
		DeferCleanup(func(ctx SpecContext) error {
			if streamClient.IsOpen() {
				_ = streamClient.DeleteStream(ctx, stream)
				return streamClient.Close(ctx)
			}
			return nil
		})

		Expect(streamClient.IsOpen()).To(BeTrue(), "expected stream client to be open")
		Expect(streamClient.ExchangeCommandVersions(ctx)).To(Succeed())

		Expect(streamClient.DeclareStream(itCtx, stream, constants.StreamConfiguration{})).To(Succeed())

		const publisherId = 2
		Expect(
			streamClient.DeclarePublisher(itCtx, publisherId, "e2e-send-and-receive", stream),
		).To(Succeed())

		c := streamClient.NotifyPublish(make(chan *raw.PublishConfirm, 100))

		const numMessages = 100
		for i := 0; i < numMessages; i++ {
			Expect(
				streamClient.Send(itCtx, publisherId, wrap[common.PublishingMessager](raw.NewPublishingMessage(uint64(i), &plainTextMessage{messageBody}))),
			).To(Succeed())
		}

		var countOfPublishingIds int
		for confirm := range c {
			Expect(confirm.PublisherID()).To(BeNumerically("==", publisherId))
			countOfPublishingIds += len(confirm.PublishingIds())
			if countOfPublishingIds >= numMessages {
				break
			}
		}

		// Assert number of PublishConfirms matches the number of messages sent
		Expect(countOfPublishingIds).To(Equal(numMessages))

		By("receiving the expected number of messages")
		chunks := streamClient.NotifyChunk(make(chan *raw.Chunk, 10))
		var subscriptionId uint8 = 2
		Expect(
			streamClient.Subscribe(itCtx, stream, constants.OffsetTypeFirst, subscriptionId, 10, nil, 0),
		).To(Succeed())

		var numOfEntries uint16 = 0
		for chunk := range chunks {
			numOfEntries += chunk.NumEntries
			debugLogger.Info("chunk received", "chunk", chunk)

			m := &plainTextMessage{}
			for i := uint16(0); i < chunk.NumEntries; i++ {
				x, z := i*104, (i+1)*104
				Expect(m.UnmarshalBinary(chunk.Messages[x:z])).To(Succeed())
				Expect(m.body).To(Equal(messageBody))
			}

			if numOfEntries >= numMessages {
				break
			}

			Expect(streamClient.Credit(ctx, subscriptionId, 10)).To(Succeed())
		}

		By("unsubscribing")
		Expect(streamClient.Unsubscribe(ctx, subscriptionId)).To(Succeed())
		//Expect(chunks).To(BeClosed())

		By("cleaning up")
		Expect(streamClient.DeletePublisher(ctx, publisherId)).To(Succeed())
		Expect(streamClient.DeleteStream(itCtx, stream)).To(Succeed())
		Expect(streamClient.Close(itCtx)).To(Succeed())
	}, SpecTimeout(15*time.Second))

})

func wrap[T any](v T) []T {
	r := make([]T, 1)
	r[0] = v
	return r
}
