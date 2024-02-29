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

var _ = Describe("Reliable Producer", func() {

	var (
		envForRProducer    *Environment
		streamForRProducer string
	)
	BeforeEach(func() {
		testEnv, err := NewEnvironment(nil)
		envForRProducer = testEnv
		Expect(err).NotTo(HaveOccurred())
		streamForRProducer = uuid.New().String()
		err = envForRProducer.DeclareStream(streamForRProducer, nil)
		Expect(err).NotTo(HaveOccurred())
	})
	AfterEach(func() {
		exists, err := envForRProducer.StreamExists(streamForRProducer)
		Expect(err).NotTo(HaveOccurred())
		if exists {
			Expect(envForRProducer.DeleteStream(streamForRProducer)).NotTo(HaveOccurred())
		}
	})

	It("Validate confirm handler", func() {
		_, err := NewReliableProducer(envForRProducer,
			streamForRProducer, &ProducerOptions{}, nil)
		Expect(err).To(HaveOccurred())
	})

	It("Create/Confirm and close a Reliable Producer", func() {
		signal := make(chan struct{})
		var confirmed int32
		producer, err := NewReliableProducer(envForRProducer,
			streamForRProducer, NewProducerOptions(), func(messageConfirm []*ConfirmationStatus) {
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
	})

	It("restart Reliable Producer in case of killing connection", func() {
		signal := make(chan struct{})
		var confirmed int32
		clientProvidedName := uuid.New().String()
		producer, err := NewReliableProducer(envForRProducer,
			streamForRProducer, NewProducerOptions().SetClientProvidedName(clientProvidedName), func(messageConfirm []*ConfirmationStatus) {
				for _, confirm := range messageConfirm {
					Expect(confirm.IsConfirmed()).To(BeTrue())
				}
				if atomic.AddInt32(&confirmed, int32(len(messageConfirm))) == 10 {
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
		for i := 0; i < 10; i++ {
			msg := amqp.NewMessage([]byte("ha"))
			err := producer.Send(msg)
			Expect(err).NotTo(HaveOccurred())
		}
		<-signal
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("Delete the stream should close the producer", func() {
		producer, err := NewReliableProducer(envForRProducer,
			streamForRProducer, NewProducerOptions(), func(messageConfirm []*ConfirmationStatus) {
			})
		Expect(err).NotTo(HaveOccurred())
		Expect(producer).NotTo(BeNil())
		Expect(envForRProducer.DeleteStream(streamForRProducer)).NotTo(HaveOccurred())
		Eventually(func() int {
			return producer.GetStatus()
		}, "15s").WithPolling(300 * time.Millisecond).Should(Equal(StatusClosed))

	})

})
