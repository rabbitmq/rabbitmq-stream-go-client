package stream

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
)

var _ = Describe("Streaming Filtering", func() {

	var (
		testEnvironment    *Environment
		testProducerStream string
	)

	BeforeEach(func() {
		client, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		testEnvironment = client
		testProducerStream = uuid.New().String()
		Expect(testEnvironment.DeclareStream(testProducerStream, nil)).
			NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(testEnvironment.DeleteStream(testProducerStream)).NotTo(HaveOccurred())
		Expect(testEnvironment.Close()).To(Succeed())
		Eventually(testEnvironment.IsClosed, time.Millisecond*300).Should(BeTrue(), "Expected testEnvironment to be closed")
	})

	It("Create Producer with Filtering", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, NewProducerOptions().SetFilter(
			NewProducerFilter(func(message message.StreamMessage) string {
				return fmt.Sprintf("%s", message.GetApplicationProperties()["id"])
			}),
		))
		Expect(err).NotTo(HaveOccurred())
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("Validate Producer/Consume Filtering not supported", func() {

		err := testEnvironment.maybeReconnectLocator()

		Expect(err).NotTo(HaveOccurred())

		// here we inject a fake response to simulate a server that
		// does not support broker filtering.
		// so we can validate it
		// This method is not thread safe and should be used only for testing purposes
		testEnvironment.locator.client.availableFeatures.brokerFilterEnabled = false

		_, err = testEnvironment.locator.client.DeclarePublisher(testProducerStream, NewProducerOptions().SetFilter(
			NewProducerFilter(func(message message.StreamMessage) string {
				return fmt.Sprintf("%s", message.GetApplicationProperties()["id"])
			}),
		))
		Expect(err).To(Equal(FilterNotSupported))

		const filterValue = "New York"
		postFilter := func(message *amqp.Message) bool {
			return message.ApplicationProperties["state"] == filterValue
		}

		filter := NewConsumerFilter([]string{filterValue}, true, postFilter)
		handleMessages := func(_ ConsumerContext, _ *amqp.Message) {}
		_, err = testEnvironment.locator.client.DeclareSubscriber(testProducerStream, handleMessages, NewConsumerOptions().SetFilter(filter))
		Expect(err).To(Equal(FilterNotSupported))

	})

	It("Send messages with Filtering", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, NewProducerOptions().SetFilter(
			NewProducerFilter(func(message message.StreamMessage) string {
				return fmt.Sprintf("%s", message.GetApplicationProperties()["id"])
			}),
		))

		for i := range 10 {
			msg := amqp.NewMessage([]byte(strconv.Itoa(i)))
			msg.ApplicationProperties = map[string]any{"id": i}
			Expect(producer.Send(msg)).NotTo(HaveOccurred())
		}

		Expect(err).NotTo(HaveOccurred())
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("Validate filter Producer/Consumer ", func() {
		const filterValue = "New York"
		postFilter := func(message *amqp.Message) bool {
			return message.ApplicationProperties["state"] == filterValue
		}

		filter := NewConsumerFilter([]string{filterValue}, true, nil)
		handleMessages := func(_ ConsumerContext, _ *amqp.Message) {}

		_, err := testEnvironment.NewConsumer(testProducerStream, handleMessages,
			NewConsumerOptions().SetFilter(filter))
		Expect(err).To(HaveOccurred())

		filter = NewConsumerFilter(nil, true, postFilter)
		_, err = testEnvironment.NewConsumer(testProducerStream, handleMessages, NewConsumerOptions().SetFilter(filter))
		Expect(err).To(HaveOccurred())

		filter = NewConsumerFilter([]string{""}, true, postFilter)
		_, err = testEnvironment.NewConsumer(testProducerStream, handleMessages, NewConsumerOptions().SetFilter(filter))
		Expect(err).To(HaveOccurred())

		// subentrybatch is not supported with filtering
		_, err = testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().
				SetFilter(NewProducerFilter(func(message message.StreamMessage) string {
					return fmt.Sprintf("%s", message.GetApplicationProperties()["id"])
				})).SetSubEntrySize(1000))
		Expect(err).To(HaveOccurred())

	})

	It("Consume messages with Filtering", func() {
		const filterValue = "New York"
		postFilterAlwaysTrue := func(_ *amqp.Message) bool {
			// always return true but the filter should be applied server side
			// so no
			return true
		}

		var consumerNewYork int32
		filter := NewConsumerFilter([]string{filterValue}, true, postFilterAlwaysTrue)
		handleMessages := func(_ ConsumerContext, _ *amqp.Message) {
			atomic.AddInt32(&consumerNewYork, 1)
		}

		consumer, err := testEnvironment.NewConsumer(testProducerStream, handleMessages,
			NewConsumerOptions().SetFilter(filter).SetOffset(OffsetSpecification{}.First()))
		Expect(err).NotTo(HaveOccurred())

		producer, err := testEnvironment.NewProducer(testProducerStream, NewProducerOptions().SetFilter(
			NewProducerFilter(func(message message.StreamMessage) string {
				return fmt.Sprintf("%s", message.GetApplicationProperties()["state"])
			}),
		))
		Expect(err).NotTo(HaveOccurred())
		send(producer, filterValue)
		// Here we wait a bit to be sure that the messages are stored in the same chunk
		time.Sleep(2 * time.Second)

		send(producer, "Alabama")   // this message should not be consumed due of the filter
		time.Sleep(1 * time.Second) // to be sure the messages are stored but won't be consumed
		Eventually(atomic.LoadInt32(&consumerNewYork) == 50, time.Millisecond*300).Should(BeTrue(),
			"Expected consumerNewYork is equal to 50")

		Expect(producer.Close()).NotTo(HaveOccurred())
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("Consume messages with Filtering and PostFilter", func() {
		const filterValue = "New York"

		// here is a post filter that will be applied after the server side filter
		// the messages are stored in the same chunk
		// so the post filter should be applied
		// here the filter server side is always true
		// so in this specific case the bloom filter is not used
		// in real case can happen that some chunks are not filtered
		// that's why the post filter is useful
		postFilterNY := func(message *amqp.Message) bool {
			return message.ApplicationProperties["state"] == filterValue
		}

		var consumerNewYork int32
		filter := NewConsumerFilter([]string{filterValue}, true, postFilterNY)
		handleMessages := func(_ ConsumerContext, _ *amqp.Message) {
			atomic.AddInt32(&consumerNewYork, 1)
		}

		consumer, err := testEnvironment.NewConsumer(testProducerStream, handleMessages,
			NewConsumerOptions().SetFilter(filter).SetOffset(OffsetSpecification{}.First()))
		Expect(err).NotTo(HaveOccurred())

		producer, err := testEnvironment.NewProducer(testProducerStream, NewProducerOptions().SetFilter(
			NewProducerFilter(func(message message.StreamMessage) string {
				return fmt.Sprintf("%s", message.GetApplicationProperties()["state"])
			}),
		))
		Expect(err).NotTo(HaveOccurred())
		send(producer, "New York")
		send(producer, "Alabama") // no sleep here, messages should end up in same chunk

		time.Sleep(2 * time.Second) // to be sure the messages are stored but won't be consumed
		Eventually(atomic.LoadInt32(&consumerNewYork) == 50, time.Millisecond*300).Should(BeTrue(),
			"Expected consumerNewYork is equal to 50")

		Expect(producer.Close()).NotTo(HaveOccurred())
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})
})

func send(producer *Producer, state string) {
	for i := 0; i < 25; i++ {
		msg := amqp.NewMessage([]byte(fmt.Sprintf("message %d, state %s", i, state)))
		msg.ApplicationProperties = map[string]any{"state": state}
		Expect(producer.Send(msg)).NotTo(HaveOccurred())
	}
	var messages []message.StreamMessage
	for i := 0; i < 25; i++ {
		msg := amqp.NewMessage([]byte(fmt.Sprintf("message %d, state %s", i, state)))
		msg.ApplicationProperties = map[string]any{"state": state}
		messages = append(messages, msg)
	}
	err := producer.BatchSend(messages)
	Expect(err).NotTo(HaveOccurred())
}
