package ha

import (
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	. "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	test_helper "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/test-helper"
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
		_, err = NewReliableConsumer(envForRConsumer, streamForRConsumer, nil, func(_ ConsumerContext, _ *amqp.Message) {
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
		for range 10 {
			msg := amqp.NewMessage([]byte("ha"))
			err := producer.Send(msg)
			Expect(err).NotTo(HaveOccurred())
		}
		<-signal
		Expect(producer.Close()).NotTo(HaveOccurred())

		signal = make(chan struct{})
		var consumed int32
		consumer, err := NewReliableConsumer(envForRConsumer, streamForRConsumer, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()), func(_ ConsumerContext, _ *amqp.Message) {
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
			func(_ ConsumerContext, _ *amqp.Message) {})
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

	It("Delete the stream should close the consumer", func() {
		consumer, err := NewReliableConsumer(envForRConsumer, streamForRConsumer,
			NewConsumerOptions(),
			func(_ ConsumerContext, _ *amqp.Message) {
			})
		Expect(err).NotTo(HaveOccurred())
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.GetStatus()).To(Equal(StatusOpen))
		Expect(consumer.GetStatusAsString()).To(Equal("Open"))
		Expect(envForRConsumer.DeleteStream(streamForRConsumer)).NotTo(HaveOccurred())
		Eventually(func() int {
			return consumer.GetStatus()
		}).WithPolling(300 * time.Millisecond).WithTimeout(20 * time.Second).Should(Equal(StatusClosed))

	})

	It("can commit a custom offset", func() {
		signal := make(chan struct{})
		var confirmed int32
		const messageToSend = 100
		producer, err := NewReliableProducer(envForRConsumer,
			streamForRConsumer, NewProducerOptions(), func(messageConfirm []*ConfirmationStatus) {
				for _, confirm := range messageConfirm {
					Expect(confirm.IsConfirmed()).To(BeTrue())
				}
				if atomic.AddInt32(&confirmed, int32(len(messageConfirm))) == messageToSend {
					signal <- struct{}{}
				}
			})
		Expect(err).NotTo(HaveOccurred())
		for range messageToSend {
			msg := amqp.NewMessage([]byte("ha"))
			err := producer.Send(msg)
			Expect(err).NotTo(HaveOccurred())
		}
		<-signal
		Expect(producer.Close()).NotTo(HaveOccurred())

		dropSignal := make(chan struct{})
		clientProvidedName := uuid.New().String()
		consumer, err := NewReliableConsumer(
			envForRConsumer,
			streamForRConsumer,
			NewConsumerOptions().
				SetOffset(OffsetSpecification{}.First()).
				SetManualCommit().
				SetConsumerName(clientProvidedName).
				SetClientProvidedName(clientProvidedName),
			func(ctx ConsumerContext, _ *amqp.Message) {
				// call on every message to test the re-connection.
				offset := ctx.Consumer.GetOffset()
				_ = ctx.Consumer.StoreCustomOffset(offset - 1) // commit all except the last one

				// wait the connection drop to ensure correct offset tracking on re-connection
				if offset == messageToSend/2 {
					<-dropSignal
				}
			},
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(consumer).NotTo(BeNil())

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
		dropSignal <- struct{}{}
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() (bool, error) { return test_helper.IsConnectionAlive(clientProvidedName, "15672") }, 10*time.Second).
			Should(BeTrue(), "check if the connection is alive")

		Eventually(func() int64 { return consumer.GetLastStoredOffset() }, 10*time.Second).
			Should(Equal(int64(98)), "Offset should be 99")

		// set a custom offset
		Expect(consumer.StoreCustomOffset(99)).NotTo(HaveOccurred())
		Eventually(func() int64 { return consumer.GetLastStoredOffset() }, 1*time.Second).
			Should(Equal(int64(99)), "Offset should be 99")

		Expect(consumer.Close()).NotTo(HaveOccurred())
	})
})
