//go:build rabbitmq.stream.system_test

package stream_test

import (
	"context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gmeasure"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/common"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/stream"
	"log/slog"
	"os"
	"sync"
	"time"
)

var _ = Describe("System tests", func() {
	const (
		streamName = "stream-system-test"
		// 100 byte message
		messageBody        = "Rabbitmq-is-awesomeRabbitmq-is-awesomeRabbitmq-is-awesomeRabbitmq-is-awesomeRabbitmq-is-awesome!!!!!"
		defaultRabbitmqUri = "rabbitmq-stream://guest:guest@localhost/%2F"
	)

	BeforeEach(func() {
		if _, isSet := os.LookupEnv(SystemTestEnvVarName); !isSet {
			Skip("System test variable to run system test not set. Skipping system tests...")
		}
	})

	It("can create and connect to a stream, publish and receive messages", func(ctx SpecContext) {
		debugLogger := slog.New(slog.NewTextHandler(GinkgoWriter, &slog.HandlerOptions{Level: slog.LevelDebug}))
		itCtx := raw.NewContextWithLogger(ctx, *debugLogger)

		By("creating a new environment")
		envConfig := stream.NewEnvironmentConfiguration(
			stream.WithUri(defaultRabbitmqUri))
		env, err := stream.NewEnvironment(itCtx, envConfig)
		Expect(err).NotTo(HaveOccurred())

		By("creating a stream")
		csOpts := stream.CreateStreamOptions{time.Second * 60, stream.Kilobyte, stream.Megabyte}
		Expect(env.CreateStream(itCtx, streamName, csOpts)).To(Succeed())

		// See https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/255
		// TODO Change this By when stream pkg producer is megered
		By("creating a raw client producer")
		throughputExp := gmeasure.NewExperiment("100-byte message throughput")
		AddReportEntry(throughputExp.Name, throughputExp)
		stopWatch := throughputExp.NewStopwatch()

		streamClientConfiguration, err := raw.NewClientConfiguration(defaultRabbitmqUri)
		Expect(err).ToNot(HaveOccurred())
		streamClient, err := raw.DialConfig(itCtx, streamClientConfiguration)
		Expect(err).ToNot(HaveOccurred())
		Expect(streamClient.IsOpen()).To(BeTrue(), "expected stream client to be open")

		const publisherId = 1
		Expect(
			streamClient.DeclarePublisher(itCtx, publisherId, "e2e-publisher", streamName),
		).To(Succeed())

		// See https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/255
		// TODO Change this By when stream pkg producer is megered
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

		// See https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/255
		// TODO Change this By when stream pkg producer is megered
		By("sending messages")
		for i := uint64(0); i < numMessages; i++ {
			messageContainer := raw.NewPublishingMessage(i, &plainTextMessage{messageBody})
			Expect(
				streamClient.Send(itCtx, publisherId, wrap[common.PublishingMessager](messageContainer)),
			).To(Succeed())
		}
		stopWatch.Record("Send").Reset()

		wg.Wait()

		By("creating a consumer")
		handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
			fmt.Printf("messages: %s\n", message.Data)
		}
		opts := &stream.ConsumerOptions{}
		mutex := &sync.Mutex{}
		consumer, err := stream.NewConsumer(streamName, streamClient, handleMessages, opts, mutex)
		Expect(err).NotTo(HaveOccurred())
		err = consumer.Subscribe(itCtx)
		Expect(err).NotTo(HaveOccurred())
		// TODO ensure all messages are consumed

		By("deleting the stream")
		Expect(env.DeleteStream(itCtx, streamName)).To(Succeed())
	})

	It("connects to RabbitMQ", func() {
		uri := "rabbitmq-stream://localhost:5552"
		conf := stream.NewEnvironmentConfiguration(
			stream.WithUri(uri),
			stream.WithLazyInitialization(false),
			stream.WithAddressResolver(func(_ string, _ int) (_ string, _ int) {
				return "localhost", 5552
			}),
			stream.WithId("system-test"),
		)
		env, err := stream.NewEnvironment(context.Background(), conf)
		Expect(err).ToNot(HaveOccurred())
		streamName := "test-stream"
		Expect(env.CreateStream(context.Background(), streamName, stream.CreateStreamOptions{})).To(Succeed())
		producer, err := env.CreateProducer(context.Background(), streamName, &stream.ProducerOptions{})
		Expect(err).ToNot(HaveOccurred())

		DeferCleanup(func() {
			producer.Close()
			env.Close(context.Background())
		})

		for i := 0; i < 1_000; i++ {
			Expect(producer.Send(context.Background(), amqp.Message{Data: []byte(fmt.Sprintf("Message #%d", i))})).To(Succeed())
		}

		_, err = env.QueryStreamStats(context.Background(), streamName)
		Expect(err).ToNot(HaveOccurred())

		Expect(env.DeleteStream(context.Background(), streamName)).To(Succeed())
	})
})

func wrap[T any](v T) []T {
	r := make([]T, 1)
	r[0] = v
	return r
}
