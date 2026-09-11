package stream_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

var _ = Describe("Close notifications after broker operations", func() {
	It("retains close events registered after the entity is closed", func() {
		env, err := stream.NewEnvironment(stream.NewEnvironmentOptions())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(env.Close()).To(Succeed()) })

		name := fmt.Sprintf("close-notification-%d", time.Now().UnixNano())
		Expect(env.DeclareStream(name, stream.NewStreamOptions())).To(Succeed())
		DeferCleanup(func() { Expect(env.DeleteStream(name)).To(Succeed()) })

		By("closing a producer before registering for its notifications")
		producer, err := env.NewProducer(name, stream.NewProducerOptions())
		Expect(err).NotTo(HaveOccurred())
		Expect(producer.Close()).To(Succeed())

		var producerEvent stream.Event
		Eventually(producer.NotifyClose(), 5*time.Second).Should(Receive(&producerEvent),
			"producer close before registration was lost")
		Expect(producerEvent.Reason).To(Equal(stream.DeletePublisher))

		By("closing a consumer before registering for its notifications")
		consumer, err := env.NewConsumer(name, func(stream.ConsumerContext, *amqp.Message) {}, stream.NewConsumerOptions())
		Expect(err).NotTo(HaveOccurred())
		Expect(consumer.Close()).To(Succeed())

		var consumerEvent stream.Event
		Eventually(consumer.NotifyClose(), 5*time.Second).Should(Receive(&consumerEvent),
			"consumer close before registration was lost")
		Expect(consumerEvent.Reason).To(Equal(stream.UnSubscribe))

		By("closing a super stream consumer before registering for its partition notifications")
		superName := name + "-super"
		Expect(env.DeclareSuperStream(superName, stream.NewPartitionsOptions(2))).To(Succeed())
		DeferCleanup(func() { Expect(env.DeleteSuperStream(superName)).To(Succeed()) })

		super, err := env.NewSuperStreamConsumer(superName, func(stream.ConsumerContext, *amqp.Message) {}, stream.NewSuperStreamConsumerOptions())
		Expect(err).NotTo(HaveOccurred())
		Expect(super.Close()).To(Succeed())

		events := super.NotifyPartitionClose(1)
		partitions := make([]string, 0, 2)
		for range 2 {
			var event stream.CPartitionClose
			Eventually(events, 5*time.Second).Should(Receive(&event),
				"partition close before registration was lost")
			partitions = append(partitions, event.Partition)
		}
		Expect(partitions).To(ConsistOf(superName+"-0", superName+"-1"))

		Eventually(events, 5*time.Second).Should(BeClosed(),
			"partition notification channel did not close")
	})
})
