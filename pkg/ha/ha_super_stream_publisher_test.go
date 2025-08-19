package ha

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	test_helper "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/test-helper"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

var _ = Describe("Reliable Super Stream Producer", func() {

	var (
		envForSuperStreamProducer    *Environment
		streamForSuperStreamProducer string
	)
	BeforeEach(func() {
		testEnv, err := NewEnvironment(nil)
		envForSuperStreamProducer = testEnv
		Expect(err).NotTo(HaveOccurred())
		streamForSuperStreamProducer = fmt.Sprintf("super_stream_producer_%s", uuid.New().String())
		err = envForSuperStreamProducer.DeclareSuperStream(streamForSuperStreamProducer, NewPartitionsOptions(3))
		Expect(err).NotTo(HaveOccurred())
	})
	AfterEach(func() {
		Expect(envForSuperStreamProducer.DeleteSuperStream(streamForSuperStreamProducer)).NotTo(HaveOccurred())
	})

	It("Validate mandatory fields", func() {
		_, err := NewReliableSuperStreamProducer(envForSuperStreamProducer,
			streamForSuperStreamProducer, nil, nil)
		Expect(err).To(HaveOccurred())
		_, err = NewReliableSuperStreamProducer(envForSuperStreamProducer,
			streamForSuperStreamProducer, &SuperStreamProducerOptions{}, nil)
		Expect(err).To(HaveOccurred())
	})

	It("Create/Confirm and close a Reliable Super Stream Producer", func() {

		signal := make(chan struct{})
		var confirmed int32
		superProducer, err := NewReliableSuperStreamProducer(envForSuperStreamProducer,
			streamForSuperStreamProducer, &SuperStreamProducerOptions{
				RoutingStrategy: NewHashRoutingStrategy(func(message message.StreamMessage) string {
					return message.GetApplicationProperties()["routingKey"].(string)
				}),
			}, func(messageConfirm []*PartitionPublishConfirm) {
				for _, confirm := range messageConfirm {
					for i, status := range confirm.ConfirmationStatus {
						Expect(status.IsConfirmed()).To(BeTrue(), fmt.Sprintf("Message %d not confirmed", i))
						if atomic.AddInt32(&confirmed, 1) == 200 {
							signal <- struct{}{}
						}
					}
				}
			})
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < 200; i++ {
			msg := amqp.NewMessage(make([]byte, 0))
			msg.ApplicationProperties = map[string]any{"routingKey": fmt.Sprintf("hello%d", i)}
			err = superProducer.Send(msg)
			Expect(err).NotTo(HaveOccurred())
		}

		<-signal
		Expect(superProducer.Close()).NotTo(HaveOccurred())
		Expect(superProducer.GetStatus()).To(Equal(StatusClosed))
		Expect(superProducer.GetStreamName()).To(Equal(streamForSuperStreamProducer))

	})

	It("restart Reliable Producer in case of killing connection", func() {

		signal := make(chan struct{})
		var confirmed int32
		clientProvidedName := uuid.New().String()
		superProducer, err := NewReliableSuperStreamProducer(envForSuperStreamProducer,
			streamForSuperStreamProducer, &SuperStreamProducerOptions{
				RoutingStrategy: NewHashRoutingStrategy(func(message message.StreamMessage) string {
					return message.GetApplicationProperties()["routingKey"].(string)
				}),
				ClientProvidedName: clientProvidedName,
			}, func(messageConfirm []*PartitionPublishConfirm) {
				for _, confirm := range messageConfirm {
					for i, status := range confirm.ConfirmationStatus {
						Expect(status.IsConfirmed()).To(BeTrue(), fmt.Sprintf("Message %d not confirmed", i))
						if atomic.AddInt32(&confirmed, 1) == 400 {
							signal <- struct{}{}
						}
					}
				}
			})
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < 200; i++ {
			msg := amqp.NewMessage(make([]byte, 0))
			msg.ApplicationProperties = map[string]any{"routingKey": fmt.Sprintf("hello%d", i)}
			err = superProducer.Send(msg)
			Expect(err).NotTo(HaveOccurred())
		}

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
		time.Sleep(500 * time.Millisecond)
		Eventually(func() int { return superProducer.GetStatus() }, "15s").WithPolling(300 * time.Millisecond).Should(Equal(StatusOpen))
		for i := 0; i < 200; i++ {
			msg := amqp.NewMessage(make([]byte, 0))
			msg.ApplicationProperties = map[string]any{"routingKey": fmt.Sprintf("hello%d", i)}
			err = superProducer.Send(msg)
			Expect(err).NotTo(HaveOccurred())
		}
		select {
		case <-signal:
		case <-time.After(5 * time.Second):
			Fail("Timeout waiting messages")
		}
		Expect(superProducer.Close()).NotTo(HaveOccurred())
		Expect(superProducer.GetStatus()).To(Equal(StatusClosed))

	})
})
