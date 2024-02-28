package ha

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	. "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
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
		Expect(envForRConsumer.DeleteStream(streamForRConsumer)).NotTo(HaveOccurred())
	})

	It("Validate confirm handler", func() {
		_, err := NewReliableConsumer(envForRConsumer,
			streamForRConsumer, &ConsumerOptions{}, nil)
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
		// create a consumer
		var consumed int32
		clientProvidedName := uuid.New().String()
		consumer, err := NewReliableConsumer(envForRConsumer, streamForRConsumer, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()).SetClientProvidedName(clientProvidedName),
			func(consumerContext ConsumerContext, message *amqp.Message) {
				time.Sleep(1 * time.Second)
				if atomic.AddInt32(&consumed, 1) == 10 {
					signal <- struct{}{}
				}
			})
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(1 * time.Second)
		connectionToDrop := ""
		Eventually(func() bool {
			connections, err := Connections("15672")
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
		errDrop := DropConnection(connectionToDrop, "15672")
		Expect(errDrop).NotTo(HaveOccurred())

		time.Sleep(2 * time.Second) // we give some time to the client to reconnect
		<-signal
		Expect(consumed).To(Equal(int32(10)))
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

})
