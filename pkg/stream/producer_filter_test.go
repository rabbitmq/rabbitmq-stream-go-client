package stream

import (
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"strconv"
	"time"
)

var _ = Describe("Streaming Producer Filtering", func() {

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
				return fmt.Sprintf("%s", message.GetApplicationProperties()["ID"])
			}),
		))
		Expect(err).NotTo(HaveOccurred())
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("Validate Producer Filtering not supported", func() {

		client, err := testEnvironment.newReconnectClient()

		Expect(err).NotTo(HaveOccurred())

		// here we inject a fake response to simulate a server that
		// does not support broker filtering.
		// so we can validate it
		// This method is not thread safe and should be used only for testing purposes
		client.availableFeatures.brokerFilterEnabled = false

		_, err = client.DeclarePublisher(testProducerStream, NewProducerOptions().SetFilter(
			NewProducerFilter(func(message message.StreamMessage) string {
				return fmt.Sprintf("%s", message.GetApplicationProperties()["ID"])
			}),
		))
		Expect(err).To(Equal(FilterNotSupported))
	})

	It("Send messages with Filtering", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, NewProducerOptions().SetFilter(
			NewProducerFilter(func(message message.StreamMessage) string {
				return fmt.Sprintf("%s", message.GetApplicationProperties()["ID"])
			}),
		))

		for i := 0; i < 10; i++ {
			msg := amqp.NewMessage([]byte(strconv.Itoa(i)))
			msg.ApplicationProperties = map[string]interface{}{"ID": i}
			Expect(producer.Send(msg)).NotTo(HaveOccurred())
		}

		Expect(err).NotTo(HaveOccurred())
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

})
