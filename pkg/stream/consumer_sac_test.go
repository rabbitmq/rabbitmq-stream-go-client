package stream

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"sync/atomic"
	"time"
)

func SendMessages(testEnvironment *Environment, streamName string) {
	producer, err := testEnvironment.NewProducer(streamName, nil)
	Expect(err).NotTo(HaveOccurred())
	Expect(producer.BatchSend(CreateArrayMessagesForTesting(30))).NotTo(HaveOccurred())
	Expect(producer.Close()).NotTo(HaveOccurred())
}

var _ = Describe("Streaming Single Active Consumer", func() {
	var (
		testEnvironment *Environment
		streamName      string
	)
	BeforeEach(func() {
		env, err := NewEnvironment(nil)
		testEnvironment = env
		Expect(err).NotTo(HaveOccurred())
		streamName = uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
	})
	AfterEach(func() {
		Expect(testEnvironment.DeleteStream(streamName)).NotTo(HaveOccurred())
	})

	It("Validate Single Active Consumer", func() {
		// name not set
		_, err := testEnvironment.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {

			}, NewConsumerOptions().SetSingleActiveConsumer(NewSingleActiveConsumer(
				func(stream string, isActive bool) OffsetSpecification {
					Expect(stream).To(Equal(streamName))
					return OffsetSpecification{}.First()
				},
			)))
		Expect(err).To(HaveOccurred())

		// string name contains spaces
		_, err2 := testEnvironment.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {

			},
			NewConsumerOptions().SetConsumerName("     ").SetSingleActiveConsumer(NewSingleActiveConsumer(
				func(_ string, isActive bool) OffsetSpecification {
					return OffsetSpecification{}.Last()
				})))
		Expect(err2).To(HaveOccurred())

		/// check support for single active consumer is not enabled
		client, err3 := testEnvironment.newReconnectClient()
		Expect(err3).NotTo(HaveOccurred())

		// here we inject a fake response to simulate a server that
		// does not support SAC .
		// so we can validate it
		// This method is not thread safe and should be used only for testing purposes
		client.availableFeatures.brokerSingleActiveConsumerEnabled = false
		handleMessages := func(consumerContext ConsumerContext, message *amqp.Message) {

		}

		_, err = client.DeclareSubscriber(streamName, handleMessages, NewConsumerOptions().SetSingleActiveConsumer(
			NewSingleActiveConsumer(func(_ string, isActive bool) OffsetSpecification {
				return OffsetSpecification{}.Last()
			}),
		))
		Expect(err).To(Equal(SingleActiveConsumerNotSupported))

		// Consumer update is nil and it must be set
		_, err4 := testEnvironment.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {

			},
			NewConsumerOptions().SetSingleActiveConsumer(NewSingleActiveConsumer(nil)))
		Expect(err4).To(HaveOccurred())
	})

	It("The second consumer should not receive messages", func() {
		// We run two consumers
		// c1 and c2
		// c1 is never closed so c2 won't be never promoted to the active consumer
		const appName = "MyApplication"
		var c1ReceivedMessages int32
		var c2ReceivedMessages int32
		c1, err := testEnvironment.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&c1ReceivedMessages, 1)
			}, NewConsumerOptions().SetConsumerName(appName).
				SetSingleActiveConsumer(NewSingleActiveConsumer(
					func(stream string, isActive bool) OffsetSpecification {
						Expect(stream).To(Equal(streamName))
						return OffsetSpecification{}.First()
					},
				)))
		Expect(err).NotTo(HaveOccurred())
		Expect(c1).NotTo(BeNil())
		Expect(c1.isActive()).To(BeTrue())

		c2, err := testEnvironment.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				// Here should never receive the messages
				// c2ReceivedMessages should be == 0
				atomic.AddInt32(&c2ReceivedMessages, 1)
			}, NewConsumerOptions().SetConsumerName(appName).
				SetSingleActiveConsumer(NewSingleActiveConsumer(
					func(_ string, isActive bool) OffsetSpecification {
						return OffsetSpecification{}.First()
					},
				)))
		Expect(err).NotTo(HaveOccurred())
		Expect(c2).NotTo(BeNil())

		SendMessages(testEnvironment, streamName)
		Eventually(atomic.LoadInt32(&c1ReceivedMessages) == 30, time.Millisecond*300).Within(5*time.Second).
			Should(BeTrue(),
				"Expected c1ReceivedMessages is equal to 30")

		Eventually(atomic.LoadInt32(&c2ReceivedMessages) == 0, time.Millisecond*300).Should(BeTrue(),
			"Expected c2ReceivedMessages should never receive messages")

		Expect(c2.Close()).NotTo(HaveOccurred())
		// After the test we can close the consumers
		Expect(c1.Close()).NotTo(HaveOccurred())
	})

	It("The second consumer should be activated and restart from an offset", func() {
		// We run two consumers
		// c1 and c2
		// c1 will be closed so the c2 will be promoted to the active consumer
		const appName = "MyApplication"
		var c1ReceivedMessages int32
		var c2ReceivedMessages int32
		c1, err := testEnvironment.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&c1ReceivedMessages, 1)
			}, NewConsumerOptions().SetConsumerName(appName).
				SetSingleActiveConsumer(NewSingleActiveConsumer(
					func(stream string, isActive bool) OffsetSpecification {
						return OffsetSpecification{}.First()
					},
				)))
		Expect(err).NotTo(HaveOccurred())
		Expect(c1).NotTo(BeNil())
		Expect(c1.isActive()).To(BeTrue())

		c2, err := testEnvironment.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&c2ReceivedMessages, 1)
			}, NewConsumerOptions().SetConsumerName(appName).
				SetSingleActiveConsumer(NewSingleActiveConsumer(
					func(stream string, isActive bool) OffsetSpecification {
						// Here the consumer is promoted it will restart from
						// offset 10
						// so c2ReceivedMessages should be 20
						return OffsetSpecification{}.Offset(10)
					},
				)))
		Expect(err).NotTo(HaveOccurred())
		Expect(c2).NotTo(BeNil())

		SendMessages(testEnvironment, streamName)
		Eventually(atomic.LoadInt32(&c1ReceivedMessages) == 30, time.Millisecond*300).Within(5*time.Second).
			Should(BeTrue(),
				"Expected c1ReceivedMessages is equal to 30")

		// we close c1 and  c2 will be activated
		Expect(c1.Close()).NotTo(HaveOccurred())

		// only 20 messages since the OffsetSpecification{}.Offset(10)
		Eventually(func() bool {
			return atomic.LoadInt32(&c2ReceivedMessages) == 20
		}, 2*time.Second).
			Within(5*time.Second).Should(BeTrue(),
			"Expected c2ReceivedMessages should receive 20 messages")

		Expect(c2.Close()).NotTo(HaveOccurred())
	})

	It("offset should not be overwritten by autocommit on consumer close when no messages have been consumed", func() {
		producer, err := testEnvironment.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		consumerUpdate := func(streamName string, isActive bool) OffsetSpecification {
			offset, err := testEnvironment.QueryOffset("my_consumer", streamName)
			if err != nil {
				return OffsetSpecification{}.First()
			}

			return OffsetSpecification{}.Offset(offset + 1)
		}

		var messagesReceived int32 = 0
		consumerA, err := testEnvironment.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesReceived, 1)
			}, NewConsumerOptions().
				SetSingleActiveConsumer(NewSingleActiveConsumer(consumerUpdate)).
				SetConsumerName("my_consumer").
				SetAutoCommit(nil))
		Expect(err).NotTo(HaveOccurred())

		Expect(producer.BatchSend(CreateArrayMessagesForTesting(10))).NotTo(HaveOccurred())
		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 5*time.Second).Should(Equal(int32(10)),
			"consumer should receive only 10 messages")

		Expect(consumerA.Close()).NotTo(HaveOccurred())
		Expect(consumerA.GetLastStoredOffset()).To(Equal(int64(9)))

		offset, err := testEnvironment.QueryOffset("my_consumer", streamName)
		Expect(err).NotTo(HaveOccurred())
		Expect(offset).To(Equal(int64(9)))

		messagesReceived = 0
		consumerB, err := testEnvironment.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesReceived, 1)
			}, NewConsumerOptions().
				SetConsumerName("my_consumer").
				SetSingleActiveConsumer(NewSingleActiveConsumer(consumerUpdate)).
				SetAutoCommit(nil))
		Expect(err).NotTo(HaveOccurred())

		Expect(consumerB.Close()).NotTo(HaveOccurred())
		time.Sleep(100 * time.Millisecond)
		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 5*time.Second).Should(Equal(int32(0)),
			"consumer should have received no messages")

		offsetAfter, err := testEnvironment.QueryOffset("my_consumer", streamName)
		Expect(err).NotTo(HaveOccurred())
		Expect(offsetAfter).To(Equal(int64(9)))

		Expect(producer.Close()).NotTo(HaveOccurred())
	})

})
