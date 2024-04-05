package ha

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	. "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/test-helper"
	"sync/atomic"
	"time"
)

var _ = Describe("Reliable Consumer", func() {

	var (
		envForRConsumer    *Environment
		streamForRConsumer string
	)
	BeforeEach(func() {
		testEnv, err := NewEnvironment(nil)
		envForRConsumer = testEnv
		Expect(err).NotTo(HaveOccurred())
		streamForRConsumer = uuid.New().String()
		err = envForRConsumer.DeclareStream(streamForRConsumer, nil)
		Expect(err).NotTo(HaveOccurred())
	})
	AfterEach(func() {
		exists, err := envForRConsumer.StreamExists(streamForRConsumer)
		Expect(err).NotTo(HaveOccurred())
		if exists {
			Expect(envForRConsumer.DeleteStream(streamForRConsumer)).NotTo(HaveOccurred())
		}
	})

	It("Validate mandatory fields", func() {
		_, err := NewReliableConsumer(envForRConsumer,
			streamForRConsumer, &ConsumerOptions{}, nil)
		Expect(err).To(HaveOccurred())
		_, err = NewReliableConsumer(envForRConsumer, streamForRConsumer, nil, func(consumerContext ConsumerContext, message *amqp.Message) {
		})
		Expect(err).To(HaveOccurred())
	})

	It("Create/Confirm and close a Reliable Producer / Consumer", func() {
		signal := make(chan struct{})
		var confirmed int32
		producer, err := NewReliableProducer(envForRConsumer,
			streamForRConsumer, NewProducerOptions(), func(messageConfirm []*ConfirmationStatus) {
				for _, confirm := range messageConfirm {
					Expect(confirm.IsConfirmed()).To(BeTrue())
				}
				if atomic.AddInt32(&confirmed, int32(len(messageConfirm))) == 10 {
					signal <- struct{}{}
				}
			})
		Expect(err).NotTo(HaveOccurred())
		for i := 0; i < 10; i++ {
			msg := amqp.NewMessage([]byte("ha"))
			err := producer.Send(msg)
			Expect(err).NotTo(HaveOccurred())
		}
		<-signal
		Expect(producer.Close()).NotTo(HaveOccurred())

		signal = make(chan struct{})
		var consumed int32
		consumer, err := NewReliableConsumer(envForRConsumer, streamForRConsumer, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()), func(consumerContext ConsumerContext, message *amqp.Message) {
			atomic.AddInt32(&consumed, 1)
			if atomic.LoadInt32(&consumed) == 10 {
				signal <- struct{}{}
			}
		})

		Expect(err).NotTo(HaveOccurred())
		<-signal
		Expect(consumed).To(Equal(int32(10)))
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("restart Reliable Consumer in case of killing connection", func() {

		clientProvidedName := uuid.New().String()
		consumer, err := NewReliableConsumer(envForRConsumer, streamForRConsumer, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()).SetClientProvidedName(clientProvidedName),
			func(consumerContext ConsumerContext, message *amqp.Message) {
			})
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

		time.Sleep(2 * time.Second) // we give some time to the client to reconnect
		Expect(consumer.GetStatus()).To(Equal(StatusOpen))
		Expect(consumer.Close()).NotTo(HaveOccurred())
		Expect(consumer.GetStatus()).To(Equal(StatusClosed))
	})

	It("Delete the stream should close the consumer", func() {
		consumer, err := NewReliableConsumer(envForRConsumer, streamForRConsumer,
			NewConsumerOptions(),
			func(consumerContext ConsumerContext, message *amqp.Message) {
			})
		Expect(err).NotTo(HaveOccurred())
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.GetStatus()).To(Equal(StatusOpen))
		Expect(envForRConsumer.DeleteStream(streamForRConsumer)).NotTo(HaveOccurred())
		Eventually(func() int {
			return consumer.GetStatus()
		}, "15s").WithPolling(300 * time.Millisecond).Should(Equal(StatusClosed))

	})
})
