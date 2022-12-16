//go:build rabbitmq.stream.e2e

package e2e_test

import (
	"github.com/go-logr/logr"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/common"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gmeasure"
	"time"
)

var _ = Describe("E2E", Serial, Label("e2e"), func() {
	const (
		stream = "e2e-stream-test"
		// 100 byte message
		messageBody = "Rabbitmq-is-awesomeRabbitmq-is-awesomeRabbitmq-is-awesomeRabbitmq-is-awesomeRabbitmq-is-awesome!!!!!"
	)

	It("connects, creates, publishes, deletes and closes", Label("measurement"), func(ctx SpecContext) {
		itCtx := logr.NewContext(ctx, GinkgoLogr)
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

		By("sending messages")
		for i := uint64(0); i < 1_000_000; i++ {
			messageContainer := raw.NewPublishingMessage(i, &plainTextMessage{messageBody})
			Expect(
				streamClient.Send(itCtx, publisherId, wrap[common.PublishingMessager](messageContainer)),
			).To(Succeed())
		}
		stopWatch.Record("Send").Reset()

		By("receiving confirmations")
		// TODO

		By("deleting the publisher")
		Expect(streamClient.DeletePublisher(ctx, publisherId)).To(Succeed())
		stopWatch.Record("DeletePublisher").Reset()

		By("deleting a stream")
		Expect(streamClient.DeleteStream(itCtx, stream)).To(Succeed())
		stopWatch.Record("DeleteStream").Reset()

		By("closing the connection")
		Expect(streamClient.Close(itCtx)).To(Succeed())
	}, SpecTimeout(120*time.Second))
})

func wrap[T any](v T) []T {
	r := make([]T, 1, 1)
	r[0] = v
	return r
}
