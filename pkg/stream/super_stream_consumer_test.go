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

	It("Validate the Super Stream Consumer", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())

		consumer, err := newSuperStreamConsumer(nil, "superStream", nil, nil)
		Expect(err).To(Equal(ErrEnvironmentNotDefined))
		Expect(consumer).To(BeNil())

		c1, err := env.NewSuperStreamConsumer("superStream", nil, nil)
		Expect(err).To(Equal(ErrSuperStreamConsumerOptionsNotDefined))
		Expect(c1).To(BeNil())

		c2, err := env.NewSuperStreamConsumer("  ", nil, NewSuperStreamConsumerOptions())
		Expect(err).To(HaveOccurred())
		Expect(c2).To(BeNil())

		c3, err := env.NewSuperStreamConsumer("", nil, NewSuperStreamConsumerOptions())
		Expect(err).To(HaveOccurred())
		Expect(c3).To(BeNil())

		Expect(env.Close()).NotTo(HaveOccurred())
	})

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
		superStreamConsumer, err := newSuperStreamConsumer(env, superStream, messagesHandler, options)
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
		superStreamConsumer, err := newSuperStreamConsumer(env, superStream, messagesHandler, options)
		Expect(err).NotTo(HaveOccurred())
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

		superStreamConsumer, err := newSuperStreamConsumer(env, superStream, messagesHandler, NewSuperStreamConsumerOptions().
			SetConsumerName("consume-20messages"))
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
		// test the SAc in different scenarios
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

			firstSuperStreamConsumer, err := newSuperStreamConsumer(env, superStream, messagesHandler, &SuperStreamConsumerOptions{
				Offset:       OffsetSpecification{}.First(),
				ConsumerName: applicationName1,
				SingleActiveConsumer: &SingleActiveConsumer{
					Enabled: isSac,
					ConsumerUpdate: func(_ string, isActive bool) OffsetSpecification {
						return OffsetSpecification{}.First()
					},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(firstSuperStreamConsumer.init()).NotTo(HaveOccurred())

			secondSuperStreamConsumer, err := newSuperStreamConsumer(env, superStream, messagesHandler, &SuperStreamConsumerOptions{
				Offset:       OffsetSpecification{}.First(),
				ConsumerName: applicationName2,
				SingleActiveConsumer: &SingleActiveConsumer{
					Enabled: isSac,
					ConsumerUpdate: func(_ string, isActive bool) OffsetSpecification {
						return OffsetSpecification{}.First()
					},
				},
			})
			Expect(err).NotTo(HaveOccurred())
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

	It("The second consumer should be activated and restart from first", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())

		superStream := "sac-super-stream-the-second-should-restart-consume"
		Expect(env.DeclareSuperStream(superStream, NewPartitionsOptions(3))).NotTo(HaveOccurred())

		const appName = "MyApplication"
		Send(env, superStream)

		receivedMap := make(map[string]int)
		var receivedMessages int32
		mutex := sync.Mutex{}
		messagesHandler := func(consumerContext ConsumerContext, message *amqp.Message) {
			mutex.Lock()
			receivedMap[consumerContext.Consumer.GetStreamName()] += 1
			mutex.Unlock()
			atomic.AddInt32(&receivedMessages, 1)
		}

		firstSuperStreamConsumer, err := env.NewSuperStreamConsumer(superStream, messagesHandler, NewSuperStreamConsumerOptions().
			SetConsumerName(appName).SetSingleActiveConsumer(
			NewSingleActiveConsumer(func(_ string, isActive bool) OffsetSpecification {
				return OffsetSpecification{}.First()
			})))
		Expect(err).NotTo(HaveOccurred())
		Expect(firstSuperStreamConsumer).NotTo(BeNil())

		secondSuperStreamConsumer, err := env.NewSuperStreamConsumer(superStream, messagesHandler, NewSuperStreamConsumerOptions().
			SetConsumerName(appName).SetSingleActiveConsumer(NewSingleActiveConsumer(
			func(streamName string, isActive bool) OffsetSpecification {
				return OffsetSpecification{}.First()
			},
		)))
		Expect(err).NotTo(HaveOccurred())
		Expect(secondSuperStreamConsumer).NotTo(BeNil())

		// only the first consumer should receive the messages
		Eventually(func() int32 { return atomic.LoadInt32(&receivedMessages) }, 300*time.Millisecond).WithTimeout(5 * time.Second).Should(Equal(int32(20)))
		Expect(receivedMap).To(HaveLen(3))
		Expect(receivedMap[fmt.Sprintf("%s-0", superStream)]).To(Equal(9))
		Expect(receivedMap[fmt.Sprintf("%s-1", superStream)]).To(Equal(7))
		Expect(receivedMap[fmt.Sprintf("%s-2", superStream)]).To(Equal(4))

		// close the first consumer and the second consumer should be promoted to the active consumer
		Expect(firstSuperStreamConsumer.Close()).NotTo(HaveOccurred())

		// the second consumer should receive the messages from the beginning
		// due of the SingleActiveConsumer configuration for the second consumer
		Eventually(func() int32 { return atomic.LoadInt32(&receivedMessages) }, 300*time.Millisecond).WithTimeout(5 * time.Second).Should(Equal(int32(40)))

		Expect(receivedMap).To(HaveLen(3))

		Expect(receivedMap[fmt.Sprintf("%s-0", superStream)]).To(Equal(18))
		Expect(receivedMap[fmt.Sprintf("%s-1", superStream)]).To(Equal(14))
		Expect(receivedMap[fmt.Sprintf("%s-2", superStream)]).To(Equal(8))

		Expect(secondSuperStreamConsumer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
	})

})
