package ha

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"

	test_helper "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/test-helper"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	. "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func sendToSuperStream(env *Environment, superStream string) error {
	signal := make(chan struct{})
	superProducer, err := env.NewSuperStreamProducer(superStream,
		&SuperStreamProducerOptions{
			RoutingStrategy: NewHashRoutingStrategy(func(message message.StreamMessage) string {
				return message.GetApplicationProperties()["routingKey"].(string)
			})})
	if err != nil {
		return fmt.Errorf("error creating super stream producer: %w", err)
	}

	go func(ch <-chan PartitionPublishConfirm) {
		recv := 0
		for confirm := range ch {
			recv += len(confirm.ConfirmationStatus)
			if recv == 200 {
				signal <- struct{}{}
			}
		}
	}(superProducer.NotifyPublishConfirmation(1))

	for i := range 200 {
		msg := amqp.NewMessage(make([]byte, 0))
		msg.ApplicationProperties = map[string]any{"routingKey": fmt.Sprintf("hello%d", i)}
		err = superProducer.Send(msg)
		if err != nil {
			return fmt.Errorf("error sending message to super stream: %w", err)
		}
	}
	<-signal
	close(signal)
	err = superProducer.Close()
	if err != nil {
		return err
	}
	return nil
}

var _ = Describe("Reliable Super Stream Consumer", func() {

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
		var received atomic.Int32
		clientProvidedName := uuid.New().String()
		// signal := make(chan struct{})
		consumer, err := NewReliableSuperStreamConsumer(envForSuperStreamConsumer, streamForSuperStreamConsumer,
			func(_ ConsumerContext, _ *amqp.Message) {
				received.Add(1)
			}, NewSuperStreamConsumerOptions().SetOffset(OffsetSpecification{}.First()).SetClientProvidedName(clientProvidedName))
		Expect(err).NotTo(HaveOccurred())
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.GetStatus()).To(Equal(StatusOpen))
		Expect(sendToSuperStream(envForSuperStreamConsumer, streamForSuperStreamConsumer)).Should(Succeed())
		time.Sleep(2 * time.Second)
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
		Eventually(func() int { return consumer.GetStatus() }, "15s").WithPolling(300 * time.Millisecond).Should(Equal(StatusOpen))
		Expect(consumer.GetStatusAsString()).To(Equal("Open"))
		time.Sleep(1000 * time.Millisecond)
		// 79,57,64
		offsetStored, errLoad := consumer.streamPositionMap.Load(fmt.Sprintf("%s-0", streamForSuperStreamConsumer))
		Expect(errLoad).To(BeTrue())
		Expect(offsetStored).NotTo(BeNil())
		Expect(offsetStored).To(Equal(int64(78)))

		offsetStored, errLoad = consumer.streamPositionMap.Load(fmt.Sprintf("%s-1", streamForSuperStreamConsumer))
		Expect(errLoad).To(BeTrue())
		Expect(offsetStored).NotTo(BeNil())
		Expect(offsetStored).To(Equal(int64(56)))

		offsetStored, errLoad = consumer.streamPositionMap.Load(fmt.Sprintf("%s-2", streamForSuperStreamConsumer))
		Expect(errLoad).To(BeTrue())
		Expect(offsetStored).NotTo(BeNil())
		Expect(offsetStored).To(Equal(int64(63)))
		Expect(received.Load()).To(Equal(int32(200)))
		Expect(consumer.Close()).NotTo(HaveOccurred())
		Expect(consumer.GetStatus()).To(Equal(StatusClosed))
		Expect(consumer.GetStatusAsString()).To(Equal("Closed"))
	})

})
