package stream

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"sync"
	"sync/atomic"
	"time"
)

func Send(env *Environment, superStream string) {

	signal := make(chan struct{})
	superProducer, err := env.NewSuperStreamProducer(superStream,
		&SuperStreamProducerOptions{
			RoutingStrategy: NewHashRoutingStrategy(func(message message.StreamMessage) string {
				return message.GetApplicationProperties()["routingKey"].(string)
			})})
	Expect(err).NotTo(HaveOccurred())

	go func(ch <-chan PartitionPublishConfirm) {
		recv := 0
		for confirm := range ch {
			recv += len(confirm.ConfirmationStatus)
			if recv == 20 {
				signal <- struct{}{}
			}
		}

	}(superProducer.NotifyPublishConfirmation())

	for i := 0; i < 20; i++ {
		msg := amqp.NewMessage(make([]byte, 0))
		msg.ApplicationProperties = map[string]interface{}{"routingKey": fmt.Sprintf("hello%d", i)}
		Expect(superProducer.Send(msg)).NotTo(HaveOccurred())
	}
	Expect(superProducer.Close()).NotTo(HaveOccurred())
	<-signal
	close(signal)

}

var _ = Describe("Super Stream Producer", Label("super-stream-consumer"), func() {

	It("should create a new super stream consumer", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())

		superStream := "first-super-stream-consumer"

		Expect(env.DeclareSuperStream(superStream, NewPartitionsOptions(3))).NotTo(HaveOccurred())

		messagesHandler := func(consumerContext ConsumerContext, message *amqp.Message) {}
		options := &SuperStreamConsumerOptions{
			ClientProvidedName: "client-provided-name",
			Offset:             OffsetSpecification{}.First(),
		}
		superStreamConsumer := newSuperStreamConsumer(env, superStream, messagesHandler, options)
		Expect(err).NotTo(HaveOccurred())
		Expect(superStreamConsumer).NotTo(BeNil())
		Expect(superStreamConsumer.SuperStream).To(Equal(superStream))
		Expect(superStreamConsumer.SuperStreamConsumerOptions).To(Equal(options))
		Expect(superStreamConsumer.activeConsumers).To(BeEmpty())
		Expect(superStreamConsumer.partitions).To(BeEmpty())
		Expect(superStreamConsumer.env).To(Equal(env))

		Expect(superStreamConsumer.init()).NotTo(HaveOccurred())
		Expect(superStreamConsumer.partitions).To(HaveLen(3))
		Expect(superStreamConsumer.activeConsumers).To(HaveLen(3))

		Expect(superStreamConsumer.Close()).NotTo(HaveOccurred())
		Expect(superStreamConsumer.activeConsumers).To(HaveLen(0))
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
	})

	It("validate super stream consumer ", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		superStream := "validate-super-stream-consumer"
		Expect(env.DeclareSuperStream(superStream, NewPartitionsOptions(3))).NotTo(HaveOccurred())

		messagesHandler := func(consumerContext ConsumerContext, message *amqp.Message) {}
		options := &SuperStreamConsumerOptions{
			ClientProvidedName: "client-provided-name",
			Offset:             OffsetSpecification{}.First(),
		}
		superStreamConsumer := newSuperStreamConsumer(env, superStream, messagesHandler, options)

		Expect(superStreamConsumer.init()).NotTo(HaveOccurred())

		// the consumer is already connected to the partition
		Expect(superStreamConsumer.ConnectPartition(fmt.Sprintf("%s-0", superStream), OffsetSpecification{}.First())).To(HaveOccurred())
		Expect(superStreamConsumer.ConnectPartition(fmt.Sprintf("%s-1", superStream), OffsetSpecification{}.First())).To(HaveOccurred())
		Expect(superStreamConsumer.ConnectPartition(fmt.Sprintf("%s-1", superStream), OffsetSpecification{}.First())).To(HaveOccurred())

		// the partition does not exist
		Expect(superStreamConsumer.ConnectPartition("partition_does_not_exist", OffsetSpecification{}.First())).To(HaveOccurred())

		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
	})

	It("consumer should consume 20 messages ", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())

		superStream := "consume-20messages-super-stream-consumer"
		Expect(env.DeclareSuperStream(superStream, NewPartitionsOptions(3))).NotTo(HaveOccurred())

		var receivedMessages int32
		receivedMap := make(map[string]int)
		mutex := sync.Mutex{}
		messagesHandler := func(consumerContext ConsumerContext, message *amqp.Message) {
			mutex.Lock()
			receivedMap[consumerContext.Consumer.GetStreamName()] += 1
			mutex.Unlock()
			atomic.AddInt32(&receivedMessages, 1)
		}
		options := &SuperStreamConsumerOptions{
			ClientProvidedName: "client-provided-name",
			Offset:             OffsetSpecification{}.First(),
		}
		superStreamConsumer := newSuperStreamConsumer(env, superStream, messagesHandler, options)
		Expect(err).NotTo(HaveOccurred())

		Expect(superStreamConsumer.init()).NotTo(HaveOccurred())
		Send(env, superStream)
		Eventually(func() int32 { return atomic.LoadInt32(&receivedMessages) }, 300*time.Millisecond).WithTimeout(5 * time.Second).Should(Equal(int32(20)))

		Expect(receivedMap).To(HaveLen(3))
		Expect(receivedMap[fmt.Sprintf("%s-0", superStream)]).To(Equal(9))
		Expect(receivedMap[fmt.Sprintf("%s-1", superStream)]).To(Equal(7))
		Expect(receivedMap[fmt.Sprintf("%s-2", superStream)]).To(Equal(4))
		Expect(superStreamConsumer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	DescribeTable("Single active consumer in action",

		func(isSac bool, totalMessagesReceived int, applicationName1 string, applicationName2 string) {

			env, err := NewEnvironment(nil)
			Expect(err).NotTo(HaveOccurred())

			superStream := "sac-super-stream-the-second-should-not-consume"
			Expect(env.DeclareSuperStream(superStream, NewPartitionsOptions(3))).NotTo(HaveOccurred())

			var receivedMessages int32

			receivedMap := make(map[string]int)
			mutex := sync.Mutex{}
			messagesHandler := func(consumerContext ConsumerContext, message *amqp.Message) {
				mutex.Lock()
				receivedMap[consumerContext.Consumer.GetStreamName()] += 1
				mutex.Unlock()
				atomic.AddInt32(&receivedMessages, 1)
			}

			firstSuperStreamConsumer := newSuperStreamConsumer(env, superStream, messagesHandler, &SuperStreamConsumerOptions{
				Offset:       OffsetSpecification{}.First(),
				ConsumerName: applicationName1,
				SingleActiveConsumer: &SingleActiveConsumer{
					Enabled: isSac,
					ConsumerUpdate: func(_ string, isActive bool) OffsetSpecification {
						return OffsetSpecification{}.First()
					},
				},
			})
			Expect(firstSuperStreamConsumer.init()).NotTo(HaveOccurred())

			secondSuperStreamConsumer := newSuperStreamConsumer(env, superStream, messagesHandler, &SuperStreamConsumerOptions{
				Offset:       OffsetSpecification{}.First(),
				ConsumerName: applicationName2,
				SingleActiveConsumer: &SingleActiveConsumer{
					Enabled: isSac,
					ConsumerUpdate: func(_ string, isActive bool) OffsetSpecification {
						return OffsetSpecification{}.First()
					},
				},
			})
			Expect(secondSuperStreamConsumer.init()).NotTo(HaveOccurred())

			Send(env, superStream)

			Eventually(func() int32 { return atomic.LoadInt32(&receivedMessages) }, 300*time.Millisecond).
				WithTimeout(5 * time.Second).Should(Equal(int32(totalMessagesReceived)))

			Expect(receivedMap).To(HaveLen(3))

			Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
			Expect(env.Close()).NotTo(HaveOccurred())
		},

		Entry("SAC not enabled. Consumers should consume independently", false, 40, "application_1", "application_1"),
		Entry("SAC enabled. Only One consumer should receive the messages", true, 20, "application_1", "application_1"),
		Entry("SAC enabled but the Names are different. Consumers should consume independently", true, 40, "application_1", "application_2"),
	)

})
