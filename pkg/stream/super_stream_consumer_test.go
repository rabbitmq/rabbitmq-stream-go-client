package stream

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	test_helper "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/test-helper"
	"strconv"
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

	}(superProducer.NotifyPublishConfirmation(1))

	for i := 0; i < 20; i++ {
		msg := amqp.NewMessage(make([]byte, 0))
		msg.ApplicationProperties = map[string]interface{}{"routingKey": fmt.Sprintf("hello%d", i)}
		Expect(superProducer.Send(msg)).NotTo(HaveOccurred())
	}
	<-signal
	close(signal)
	Expect(superProducer.Close()).NotTo(HaveOccurred())

}

var _ = Describe("Super Stream Consumer", Label("super-stream-consumer"), func() {

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

		superStream := fmt.Sprintf("first-super-stream-consumer-%d", time.Now().Unix())
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
		superStream := fmt.Sprintf("validate-super-stream-consumer-%d", time.Now().Unix())
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

		superStream := fmt.Sprintf("consume-20messages-super-stream-consumer-%d", time.Now().Unix())
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

	DescribeTable("Single active consumer in action", Label("super-stream-consumer"),
		// test the SAC in different scenarios
		func(isSac bool, totalMessagesReceived int, applicationName1 string, applicationName2 string, id string) {

			env, err := NewEnvironment(nil)
			Expect(err).NotTo(HaveOccurred())

			superStream := fmt.Sprintf("sac-super-stream-the-second-consumer-in-action-%s-%d", id, time.Now().Unix())
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

			consumer, err := newSuperStreamConsumer(env, superStream, messagesHandler, &SuperStreamConsumerOptions{
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
			Expect(consumer.init()).NotTo(HaveOccurred())

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

			time.Sleep(2 * time.Second)
			Eventually(func() int32 { return atomic.LoadInt32(&receivedMessages) }).WithPolling(300 * time.Millisecond).
				WithTimeout(5 * time.Second).Should(Equal(int32(totalMessagesReceived)))

			Expect(receivedMap).To(HaveLen(3))

			Expect(consumer.Close()).NotTo(HaveOccurred())
			Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
			time.Sleep(1 * time.Second)
			Expect(env.Close()).NotTo(HaveOccurred())
		},

		Entry("SAC not enabled. Consumers should consume independently", false, 40, "application_1", "application_1", "1"),
		Entry("SAC enabled. Both should receive the messages the total messages", true, 20, "application_1", "application_1", "2"),
		Entry("SAC enabled but the Names are different. Consumers should consume independently", true, 40, "application_1", "application_2", "3"),
	)

	It("should handle reconnect the consumer for the partition ", func() {
		// The scope is to test the reconnection of the consumer
		// with context CPartitionContext

		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		var superStream = fmt.Sprintf("reconnect-super-stream-consumer-%d", time.Now().Unix())

		Expect(env.DeclareSuperStream(superStream, NewPartitionsOptions(3).
			SetBalancedLeaderLocator())).NotTo(HaveOccurred())

		var reconnectedMap = make(map[string]bool)
		mutex := sync.Mutex{}
		superConsumer, err := newSuperStreamConsumer(env, superStream, nil,
			NewSuperStreamConsumerOptions().
				SetClientProvidedName("reconnect-super-stream-consumer"))

		Expect(err).To(BeNil())
		Expect(superConsumer).NotTo(BeNil())
		Expect(superConsumer.init()).NotTo(HaveOccurred())

		go func(ch <-chan CPartitionClose) {
			defer GinkgoRecover()
			for chq := range ch {
				if chq.Event.Reason == SocketClosed {
					time.Sleep(2 * time.Second)
					Expect(chq.Context.ConnectPartition(chq.Partition,
						OffsetSpecification{}.First())).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
					mutex.Lock()
					reconnectedMap[chq.Partition] = true
					mutex.Unlock()
				}
			}

		}(superConsumer.NotifyPartitionClose(1))

		Eventually(func() error {
			return test_helper.DropConnectionClientProvidedName("reconnect-super-stream-consumer", "15672")
		}).WithPolling(300 * time.Millisecond).WithTimeout(8 * time.Second).ShouldNot(HaveOccurred())

		Eventually(func() bool {
			return len(superConsumer.getConsumers()) == 2
		}).WithPolling(300 * time.Millisecond).WithTimeout(5 * time.Second).Should(BeTrue())

		Eventually(func() bool { mutex.Lock(); defer mutex.Unlock(); return len(reconnectedMap) == 1 }).WithPolling(300 * time.Millisecond).WithTimeout(5 * time.Second).Should(BeTrue())

		Eventually(func() bool {
			return len(superConsumer.getConsumers()) == 3
		}).WithPolling(300 * time.Millisecond).WithTimeout(5 * time.Second).Should(BeTrue())

		Expect(superConsumer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("The second consumer should be activated and restart from first", func() {

		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())

		superStream := fmt.Sprintf("sac-super-stream-the-second-should-restart-consume-%d", time.Now().Unix())
		Expect(env.DeclareSuperStream(superStream, NewPartitionsOptions(2))).NotTo(HaveOccurred())

		const appName = "MyApplication"

		receivedMap := make(map[string]int)
		var receivedMessages int32
		mutex := sync.Mutex{}
		messagesHandler := func(consumerContext ConsumerContext, message *amqp.Message) {
			mutex.Lock()
			receivedMap[consumerContext.Consumer.GetStreamName()] += 1
			mutex.Unlock()
			atomic.AddInt32(&receivedMessages, 1)
		}
		// both consumers should consume the total 20 messages
		// they start from the beginning of the stream
		// firstSuperStreamConsumer takes one partition and
		// the secondSuperStreamConsumer takes the second partition
		firstSuperStreamConsumer, err := env.NewSuperStreamConsumer(superStream, messagesHandler,
			NewSuperStreamConsumerOptions().
				SetConsumerName(appName).SetSingleActiveConsumer(
				NewSingleActiveConsumer(func(_ string, isActive bool) OffsetSpecification {
					return OffsetSpecification{}.First()
				})))
		Expect(err).NotTo(HaveOccurred())
		Expect(firstSuperStreamConsumer).NotTo(BeNil())
		// the secondSuperStreamConsumer takes the second partition
		secondSuperStreamConsumer, err := env.NewSuperStreamConsumer(superStream, messagesHandler, NewSuperStreamConsumerOptions().
			SetConsumerName(appName).SetSingleActiveConsumer(NewSingleActiveConsumer(
			func(streamName string, isActive bool) OffsetSpecification {
				return OffsetSpecification{}.First()
			},
		)))
		Expect(err).NotTo(HaveOccurred())
		Expect(secondSuperStreamConsumer).NotTo(BeNil())
		Send(env, superStream)
		// both should consume the total 20 messages
		Eventually(func() int32 { return atomic.LoadInt32(&receivedMessages) }, 300*time.Millisecond).WithTimeout(5 * time.Second).Should(Equal(int32(20)))
		Expect(receivedMap).To(HaveLen(2))
		Expect(receivedMap[fmt.Sprintf("%s-0", superStream)]).To(Equal(10))
		Expect(receivedMap[fmt.Sprintf("%s-1", superStream)]).To(Equal(10))

		// close the first consumer and the second consumer should be promoted to the active consumer
		// the secondSuperStreamConsumer already consumed the messages from the beginning from one partition
		// here the other partition (the one held by the fist consumer) should be consumed from the beginning
		Expect(firstSuperStreamConsumer.Close()).NotTo(HaveOccurred())

		time.Sleep(2 * time.Second)
		// the second consumer should receive the messages from the beginning
		// due of the SingleActiveConsumer configuration for the second consumer
		// 30 =
		// - 10 from the first consumer  ( partition 1)
		// - 10 from the second consumer ( partition 2)
		// - 10 from the second consumer ( partition 1) after the first consumer is closed
		Eventually(func() int32 { return atomic.LoadInt32(&receivedMessages) }).WithPolling(300 * time.Millisecond).WithTimeout(5 * time.Second).Should(Equal(int32(30)))

		Expect(receivedMap).To(HaveLen(2))

		Expect(secondSuperStreamConsumer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	//
	It("Super Stream Filtering should consume only Italy", func() {

		// test the filtering with super stream
		// the filter is covered in the filter_test.go
		// here is just to be sure the filter is applied in the super stream
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())

		superStream := fmt.Sprintf("filtering-super-stream-should-consume-only-one-country-%d", time.Now().Unix())
		Expect(env.DeclareSuperStream(superStream,
			NewPartitionsOptions(2))).NotTo(HaveOccurred())

		superProducer, err := env.NewSuperStreamProducer(superStream, NewSuperStreamProducerOptions(
			NewHashRoutingStrategy(func(message message.StreamMessage) string {
				return message.GetMessageProperties().GroupID
			})).SetFilter(NewProducerFilter(func(m message.StreamMessage) string {
			return fmt.Sprintf("%s", m.GetApplicationProperties()["county"])
		})))
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < 7; i++ {
			msg := amqp.NewMessage(make([]byte, 0))
			msg.ApplicationProperties = map[string]interface{}{"county": "italy"}
			msg.Properties = &amqp.MessageProperties{
				GroupID: "group_first",
			}
			Expect(superProducer.Send(msg)).NotTo(HaveOccurred())
		}

		// the sleep is to be sure the messages are stored in a chunk
		// so the filter will be applied, so the first chunk will contain only Italy
		time.Sleep(1500 * time.Millisecond)

		for i := 0; i < 6; i++ {
			msg := amqp.NewMessage(make([]byte, 0))
			msg.ApplicationProperties = map[string]interface{}{"county": "spain"}
			msg.Properties = &amqp.MessageProperties{
				GroupID: "group_first",
			}
			Expect(superProducer.Send(msg)).NotTo(HaveOccurred())
		}

		time.Sleep(1500 * time.Millisecond)

		// we don't need to apply any post filter here
		// the server side filter is enough
		var consumerItaly int32
		filter := NewConsumerFilter([]string{"italy"}, false, func(message *amqp.Message) bool {
			return true
		})

		handleMessages := func(consumerContext ConsumerContext, message *amqp.Message) {
			atomic.AddInt32(&consumerItaly, 1)
		}

		superStreamConsumer, err := env.NewSuperStreamConsumer(superStream, handleMessages,
			NewSuperStreamConsumerOptions().SetFilter(filter).SetOffset(OffsetSpecification{}.First()))
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(500 * time.Millisecond)
		Eventually(func() int32 { return atomic.LoadInt32(&consumerItaly) }).WithPolling(300 * time.Millisecond).WithTimeout(5 * time.Second).Should(Equal(int32(7)))

		Expect(superProducer.Close()).NotTo(HaveOccurred())
		Expect(superStreamConsumer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("Super Stream Filtering should consume only Italy Post Filter", func() {

		// test the filtering with super stream
		// the filter is covered in the filter_test.go
		// here is just to be sure the filter is applied in the super stream
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())

		superStream := fmt.Sprintf("filtering-super-stream-should-consume-only-one-country-%d", time.Now().Unix())
		Expect(env.DeclareSuperStream(superStream,
			NewPartitionsOptions(2))).NotTo(HaveOccurred())

		superProducer, err := env.NewSuperStreamProducer(superStream, NewSuperStreamProducerOptions(
			NewHashRoutingStrategy(func(message message.StreamMessage) string {
				return message.GetMessageProperties().GroupID
			})).SetFilter(NewProducerFilter(func(m message.StreamMessage) string {
			return fmt.Sprintf("%s", m.GetApplicationProperties()["county"])
		})))
		Expect(err).NotTo(HaveOccurred())

		// In this test we mix the chunks with different types
		// The scope is to test the post filter function
		// in total we have:
		// 5 messages ITALY + 5 messages SPAIN in the same chunk
		// WAIT
		// then 10 messages SPAIN in another chunk
		// We should receive only the ITALY messages from the first chunk
		// total 10 messages
		for i := 0; i < 5; i++ {
			msgItaly := amqp.NewMessage(make([]byte, 0))
			msgItaly.ApplicationProperties = map[string]interface{}{"county": "italy"}
			msgItaly.Properties = &amqp.MessageProperties{
				GroupID: "group_first",
			}
			Expect(superProducer.Send(msgItaly)).NotTo(HaveOccurred())

			msgSpain := amqp.NewMessage(make([]byte, 0))
			msgSpain.ApplicationProperties = map[string]interface{}{"county": "spain"}
			msgSpain.Properties = &amqp.MessageProperties{
				GroupID: "group_first",
			}
			Expect(superProducer.Send(msgSpain)).NotTo(HaveOccurred())

		}

		// the sleep is to be sure the messages are stored in a chunk
		// so the filter will be applied, so the first chunk will contain only Italy
		time.Sleep(1 * time.Second)

		for i := 0; i < 10; i++ {
			msg := amqp.NewMessage(make([]byte, 0))
			msg.ApplicationProperties = map[string]interface{}{"county": "spain"}
			msg.Properties = &amqp.MessageProperties{
				GroupID: "group_first",
			}
			Expect(superProducer.Send(msg)).NotTo(HaveOccurred())
		}

		time.Sleep(500 * time.Millisecond)

		// we don't need to apply any post filter here
		// the server side filter is enough
		var consumerItaly int32
		filter := NewConsumerFilter([]string{"italy"}, false,
			func(message *amqp.Message) bool {

				return message.ApplicationProperties["county"] == "italy"
			})

		handleMessages := func(consumerContext ConsumerContext, message *amqp.Message) {
			atomic.AddInt32(&consumerItaly, 1)
		}

		superStreamConsumer, err := env.NewSuperStreamConsumer(superStream, handleMessages,
			NewSuperStreamConsumerOptions().SetFilter(filter).SetOffset(OffsetSpecification{}.First()))
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(500 * time.Millisecond)
		// we should receive only the Italy messages from the first chunk
		// The first chunk is filter with the post filter function
		// the second chuck won't send ( even in this test there is no evidence about that there is the test above about that)

		Eventually(func() int32 { return atomic.LoadInt32(&consumerItaly) }).
			WithPolling(300 * time.Millisecond).WithTimeout(5 * time.Second).Should(Equal(int32(5)))

		Expect(superProducer.Close()).NotTo(HaveOccurred())
		Expect(superStreamConsumer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("Super Stream Consumer AutoCommit", func() {
		// test the auto commit
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())

		superStream := fmt.Sprintf("super-stream-consumer-with-autocommit-%d", time.Now().Unix())
		Expect(env.DeclareSuperStream(superStream,
			NewPartitionsOptions(2))).NotTo(HaveOccurred())

		superProducer, err := env.NewSuperStreamProducer(superStream, NewSuperStreamProducerOptions(
			NewHashRoutingStrategy(func(message message.StreamMessage) string {
				return message.GetMessageProperties().GroupID
			})))
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < 20; i++ {
			msg := amqp.NewMessage(make([]byte, 0))
			msg.Properties = &amqp.MessageProperties{
				GroupID: strconv.Itoa(i % 2),
			}
			Expect(superProducer.Send(msg)).NotTo(HaveOccurred())
		}

		var receivedMessages int32
		handleMessages := func(consumerContext ConsumerContext, message *amqp.Message) {
			atomic.AddInt32(&receivedMessages, 1)
		}

		superStreamConsumer, err := env.NewSuperStreamConsumer(superStream, handleMessages,
			NewSuperStreamConsumerOptions().
				SetOffset(OffsetSpecification{}.First()).
				SetConsumerName("auto-commit-consumer").
				// the setting is to trigger the auto commit based on the message count
				// the consumer will commit the offset after 9 messages
				SetAutoCommit(&AutoCommitStrategy{
					messageCountBeforeStorage: 9,
					// flushInterval is set to 50 seconds. So it will be ignored
					// messageCountBeforeStorage will be triggered first
					flushInterval: 50 * time.Second,
				}))
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(1 * time.Second)
		Eventually(func() int32 {
			return atomic.LoadInt32(&receivedMessages)
		}).
			WithPolling(300 * time.Millisecond).WithTimeout(5 * time.Second).Should(Equal(int32(20)))

		// Given the partition routing strategy, the consumer will receive 10 messages from each partition
		// the consumer triggers the auto-commit after 9 messages.
		// So the query offset should return 8 for each partition
		offset0, err := env.QueryOffset("auto-commit-consumer", fmt.Sprintf("%s-0", superStream))
		Expect(err).NotTo(HaveOccurred())
		offset1, err := env.QueryOffset("auto-commit-consumer", fmt.Sprintf("%s-1", superStream))
		Expect(err).NotTo(HaveOccurred())

		Expect(offset0).NotTo(BeNil())
		Expect(offset0).To(Equal(int64(8)))

		Expect(offset1).NotTo(BeNil())
		Expect(offset1).To(Equal(int64(8)))

		Expect(superProducer.Close()).NotTo(HaveOccurred())
		Expect(superStreamConsumer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

})
