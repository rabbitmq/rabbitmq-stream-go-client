package ha

import (
	"fmt"
	test_helper "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/test-helper"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	. "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

var _ = Describe("Reliable Super Stream Consumer", Focus, func() {

	var (
		envForSuperStreamConsumer    *Environment
		streamForSuperStreamConsumer string
	)
	BeforeEach(func() {
		testEnv, err := NewEnvironment(nil)
		envForSuperStreamConsumer = testEnv
		Expect(err).NotTo(HaveOccurred())
		streamForSuperStreamConsumer = fmt.Sprintf("super_stream_consumer_%s", uuid.New().String())
		err = envForSuperStreamConsumer.DeclareSuperStream(streamForSuperStreamConsumer, NewPartitionsOptions(3))
		Expect(err).NotTo(HaveOccurred())
	})
	AfterEach(func() {
		Expect(envForSuperStreamConsumer.DeleteSuperStream(streamForSuperStreamConsumer)).NotTo(HaveOccurred())
	})

	It("Validate mandatory fields", func() {
		_, err := NewReliableSuperStreamConsumer(envForSuperStreamConsumer,
			"not important", func(_ ConsumerContext, _ *amqp.Message) {

			}, nil)
		Expect(err).To(HaveOccurred())

		_, err = NewReliableSuperStreamConsumer(envForSuperStreamConsumer,
			"not important", nil, &SuperStreamConsumerOptions{})
		Expect(err).To(HaveOccurred())
	})

	It("Validate status", func() {
		rSuperStreamConsumer, err := NewReliableSuperStreamConsumer(envForSuperStreamConsumer,
			streamForSuperStreamConsumer, func(_ ConsumerContext, _ *amqp.Message) {

			}, &SuperStreamConsumerOptions{
				Offset: OffsetSpecification{}.First(),
			})
		Expect(err).NotTo(HaveOccurred())
		Expect(rSuperStreamConsumer.getInfo()).NotTo(BeNil())
		Expect(rSuperStreamConsumer.GetStatus()).NotTo(BeNil())
		Expect(rSuperStreamConsumer.GetStatus()).To(Equal(StatusOpen))
		Expect(rSuperStreamConsumer.Close()).NotTo(HaveOccurred())
	})

	It("restart Reliable Consumer in case of killing connection", func() {

		clientProvidedName := uuid.New().String()
		consumer, err := NewReliableSuperStreamConsumer(envForSuperStreamConsumer, streamForSuperStreamConsumer,
			func(_ ConsumerContext, _ *amqp.Message) {}, NewSuperStreamConsumerOptions().SetOffset(OffsetSpecification{}.First()).SetClientProvidedName(clientProvidedName))
		Expect(err).NotTo(HaveOccurred())
		Expect(consumer).NotTo(BeNil())
		time.Sleep(1 * time.Second)
		Expect(consumer.GetStatus()).To(Equal(StatusOpen))
		connectionToDrop := ""
		Eventually(func() bool {
			connections, err := test_helper.Connections("15672")
			if err != nil {
				return false
			}
			for _, connection := range connections {
				if connection.ClientProperties.Connection_name == clientProvidedName {
					connectionToDrop = connection.Name
					return true
				}
			}
			return false
		}, time.Second*5).
			Should(BeTrue())

		Expect(connectionToDrop).NotTo(BeEmpty())
		// kill the connection
		errDrop := test_helper.DropConnection(connectionToDrop, "15672")
		Expect(errDrop).NotTo(HaveOccurred())
		/// just give some time to raise the event
		time.Sleep(1200 * time.Millisecond)
		Eventually(func() int { return consumer.GetStatus() }, "15s").WithPolling(300 * time.Millisecond).Should(Equal(StatusOpen))
		Expect(consumer.GetStatusAsString()).To(Equal("Open"))
		Expect(consumer.Close()).NotTo(HaveOccurred())
		Expect(consumer.GetStatus()).To(Equal(StatusClosed))
		Expect(consumer.GetStatusAsString()).To(Equal("Closed"))
	})

})
