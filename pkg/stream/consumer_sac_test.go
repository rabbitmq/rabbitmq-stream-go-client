package stream

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
)

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

			}, NewConsumerOptions().EnableSingleActiveConsumer())
		Expect(err).To(HaveOccurred())

		// string name contains spaces
		_, err2 := testEnvironment.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {

			}, NewConsumerOptions().EnableSingleActiveConsumer().SetConsumerName("   "))
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

		_, err = client.DeclareSubscriber(streamName, handleMessages, NewConsumerOptions().EnableSingleActiveConsumer())
		Expect(err).To(Equal(SingleActiveConsumerNotSupported))

	})

})
