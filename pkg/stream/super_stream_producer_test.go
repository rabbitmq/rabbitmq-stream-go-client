package stream

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	test_helper "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/test-helper"
)

type TestingRandomStrategy struct {
}

func (r *TestingRandomStrategy) Route(_ message.StreamMessage, partitions []string) ([]string, error) {
	return []string{partitions[rand.Intn(len(partitions))]}, nil
}

// not used
func (r *TestingRandomStrategy) SetRouteParameters(_ string, _ func(superStream string, routingKey string) ([]string, error)) {

}

func NewTestingRandomStrategy() *TestingRandomStrategy {
	return &TestingRandomStrategy{}
}

var _ = Describe("Super Stream Producer", Label("super-stream-producer"), func() {

	DescribeTable("Partitioning using Murmur3",

		// validate the Murmur hash function
		// the table is the same for .NET,Python,Java stream clients
		// The aim for this test is to validate the correct routing with the same keys
		func(key string, partition string) {

			routingMurmur := NewHashRoutingStrategy(func(message message.StreamMessage) string {
				return message.GetApplicationProperties()["routingKey"].(string)
			})
			Expect(routingMurmur).NotTo(BeNil())

			partitions := []string{"invoices-01", "invoices-02", "invoices-03"}

			msg := amqp.NewMessage(make([]byte, 0))
			msg.ApplicationProperties = map[string]interface{}{"routingKey": key}
			_, err := msg.MarshalBinary()
			Expect(err).NotTo(HaveOccurred())
			routing, err := routingMurmur.Route(msg, partitions)
			Expect(err).NotTo(HaveOccurred())
			Expect(routing).To(HaveLen(1))
			Expect(routing[0]).To(Equal(partition))
		},
		Entry("hello2", "hello2", "invoices-01"),
		Entry("hello1", "hello1", "invoices-02"),
		Entry("hello3", "hello3", "invoices-02"),
		Entry("hello4", "hello4", "invoices-03"),
		Entry("hello5", "hello5", "invoices-01"),
		Entry("hello6", "hello6", "invoices-03"),
		Entry("hello7", "hello7", "invoices-01"),
		Entry("hello8", "hello8", "invoices-02"),
		Entry("hello9", "hello9", "invoices-01"),
		Entry("hello10", "hello10", "invoices-03"),
		Entry("hello88", "hello88", "invoices-02"),
	)

	It("validate super stream creation", func() {
		// Validate the producer creation with invalid parameters
		// the env is not defined
		producer, err := newSuperStreamProducer(nil, "it_does_not_matter", nil)
		Expect(producer).To(BeNil())
		Expect(err).To(Equal(ErrEnvironmentNotDefined))

		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())

		// the super stream name is not defined
		producer, err = newSuperStreamProducer(env, "", nil)
		Expect(producer).To(BeNil())
		Expect(err).To(HaveOccurred())

		// the super stream name contains spaces only
		producer, err = newSuperStreamProducer(env, "    ", nil)
		Expect(producer).To(BeNil())
		Expect(err).To(HaveOccurred())

		// the options are not defined
		producer, err = newSuperStreamProducer(env, "it_does_not_matter", nil)
		Expect(producer).To(BeNil())
		Expect(err).To(Equal(ErrSuperStreamProducerOptionsNotDefined))

		// the routing strategy is not defined
		producer, err = newSuperStreamProducer(env, "it_does_not_matter", &SuperStreamProducerOptions{})
		Expect(producer).To(BeNil())
		Expect(err).To(Equal(ErrSuperStreamProducerOptionsNotDefined))

	})

	It("should create a new super stream producer", func() {
		// Simple test to validate the creation of the producer

		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		var superStream = fmt.Sprintf("first-super-stream-producer-%d", time.Now().Unix())
		Expect(env.DeclareSuperStream(superStream,
			NewPartitionsOptions(3).
				SetBalancedLeaderLocator().
				SetMaxAge(3*time.Hour).
				SetMaxLengthBytes(ByteCapacity{}.GB(1)).
				SetMaxSegmentSizeBytes(ByteCapacity{}.KB(1024)),
		)).NotTo(HaveOccurred())
		superProducer, err := newSuperStreamProducer(env, superStream, &SuperStreamProducerOptions{
			RoutingStrategy: NewHashRoutingStrategy(func(message message.StreamMessage) string {
				return message.GetApplicationProperties()["routingKey"].(string)
			}),
		})
		Expect(err).To(BeNil())
		Expect(superProducer).NotTo(BeNil())
		Expect(superProducer.init()).NotTo(HaveOccurred())
		Expect(superProducer.activeProducers).To(HaveLen(3))
		Expect(superProducer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("should Send messages and confirmed to all the streams", func() {
		// The same as Partitioning using Murmur3
		// but using the producer to send the messages and confirm the messages
		// from the partitions

		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		// we do this test to be sure that the producer is able to Send messages to all the partitions
		// the same was done in .NET client and python client
		var superStream = fmt.Sprintf("super-stream-send-messages-to-all-partitions-%d", time.Now().Unix())

		msgReceived := make(map[string]int)
		mutex := sync.Mutex{}
		Expect(env.DeclareSuperStream(superStream, NewPartitionsOptions(3))).NotTo(HaveOccurred())
		superProducer, err := env.NewSuperStreamProducer(superStream,
			&SuperStreamProducerOptions{
				RoutingStrategy: NewHashRoutingStrategy(func(message message.StreamMessage) string {
					return message.GetApplicationProperties()["routingKey"].(string)
				})})

		Expect(err).To(BeNil())
		Expect(superProducer).NotTo(BeNil())
		Expect(superProducer.activeProducers).To(HaveLen(3))

		go func(ch <-chan PartitionPublishConfirm) {
			//defer GinkgoRecover()
			for superStreamPublishConfirm := range ch {
				Expect(superStreamPublishConfirm).NotTo(BeNil())
				for _, status := range superStreamPublishConfirm.ConfirmationStatus {
					Expect(status).NotTo(BeNil())
					Expect(status.IsConfirmed()).To(BeTrue())
				}
				mutex.Lock()
				msgReceived[superStreamPublishConfirm.Partition] += len(superStreamPublishConfirm.ConfirmationStatus)
				logs.LogInfo("Partition %s confirmed %d messages, total %d",
					superStreamPublishConfirm.Partition, len(superStreamPublishConfirm.ConfirmationStatus), msgReceived[superStreamPublishConfirm.Partition])
				mutex.Unlock()
			}

		}(superProducer.NotifyPublishConfirmation(0))

		for i := 0; i < 20; i++ {
			msg := amqp.NewMessage(make([]byte, 0))
			msg.ApplicationProperties = map[string]interface{}{"routingKey": fmt.Sprintf("hello%d", i)}
			Expect(superProducer.Send(msg)).NotTo(HaveOccurred())
		}
		time.Sleep(1 * time.Second)
		// these values are the same for .NET,Python,Java stream clients
		// The aim for this test is to validate the correct routing with the
		// MurmurStrategy.
		Eventually(func() int {
			mutex.Lock()
			defer mutex.Unlock()
			logs.LogInfo("Partition 0 confirmed %d messages", msgReceived[fmt.Sprintf("%s-%s", superStream, "0")])
			return msgReceived[fmt.Sprintf("%s-%s", superStream, "0")]
		}).WithPolling(300 * time.Millisecond).WithTimeout(2 * time.Second).Should(Equal(9))
		Eventually(func() int {
			mutex.Lock()
			defer mutex.Unlock()
			logs.LogInfo("Partition 1 confirmed %d messages", msgReceived[fmt.Sprintf("%s-%s", superStream, "1")])
			return msgReceived[fmt.Sprintf("%s-%s", superStream, "1")]
		}).WithPolling(300 * time.Millisecond).WithTimeout(2 * time.Second).Should(Equal(7))
		Eventually(func() int {
			mutex.Lock()
			defer mutex.Unlock()
			logs.LogInfo("Partition 2 confirmed %d messages", msgReceived[fmt.Sprintf("%s-%s", superStream, "2")])
			return msgReceived[fmt.Sprintf("%s-%s", superStream, "2")]
		}).WithPolling(300 * time.Millisecond).WithTimeout(2 * time.Second).Should(Equal(4))

		Expect(superProducer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("should handle three close ( one for partition )", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		var superStream = fmt.Sprintf("close-super-stream-producer-%d", time.Now().Unix())
		var closedMap = make(map[string]bool)
		mutex := sync.Mutex{}
		Expect(env.DeclareSuperStream(superStream,
			NewPartitionsOptions(3).
				SetClientLocalLocator())).NotTo(HaveOccurred())
		superProducer, err := newSuperStreamProducer(env, superStream, NewSuperStreamProducerOptions(
			NewHashRoutingStrategy(func(message message.StreamMessage) string {
				return message.GetApplicationProperties()["routingKey"].(string)
			})))

		go func(ch <-chan PPartitionClose) {
			defer GinkgoRecover()
			for chq := range ch {
				mutex.Lock()
				Expect(chq.Event.Reason).To(Equal("deletePublisher"))
				closedMap[chq.Partition] = true
				mutex.Unlock()
			}

		}(superProducer.NotifyPartitionClose(1))

		Expect(superProducer).NotTo(BeNil())
		Expect(err).To(BeNil())
		Expect(superProducer.init()).NotTo(HaveOccurred())
		Expect(superProducer.activeProducers).To(HaveLen(3))
		Expect(superProducer.Close()).NotTo(HaveOccurred())

		Eventually(func() bool { mutex.Lock(); defer mutex.Unlock(); return len(closedMap) == 3 },
			300*time.Millisecond).WithTimeout(5 * time.Second).Should(BeTrue())

		Expect(superProducer.activeProducers).To(HaveLen(0))
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("should handle reconnect the producer for the partition ", func() {
		// The scope is to test the reconnection of the producer
		// with context PPartitionContext

		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		var superStream = fmt.Sprintf("reconnect-super-stream-producer-%d", time.Now().Unix())

		Expect(env.DeclareSuperStream(superStream, NewPartitionsOptions(3).
			SetBalancedLeaderLocator())).NotTo(HaveOccurred())

		var reconnectedMap = make(map[string]bool)
		mutex := sync.Mutex{}
		superProducer, err := newSuperStreamProducer(env, superStream, &SuperStreamProducerOptions{
			RoutingStrategy: NewHashRoutingStrategy(func(message message.StreamMessage) string {
				return message.GetApplicationProperties()["routingKey"].(string)
			}),

			ClientProvidedName: "reconnect-super-stream-producer",
		})
		Expect(superProducer).NotTo(BeNil())
		Expect(err).To(BeNil())
		Expect(superProducer.init()).NotTo(HaveOccurred())

		go func(ch <-chan PPartitionClose) {
			defer GinkgoRecover()
			for chq := range ch {
				if chq.Event.Reason == SocketClosed {
					time.Sleep(2 * time.Second)
					err := chq.Context.ConnectPartition(chq.Partition)
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
					mutex.Lock()
					reconnectedMap[chq.Partition] = true
					mutex.Unlock()

				}
			}

		}(superProducer.NotifyPartitionClose(1))

		time.Sleep(3 * time.Second)
		Eventually(func() error {
			return test_helper.DropConnectionClientProvidedName("reconnect-super-stream-producer", "15672")
		}, 300*time.Millisecond).WithTimeout(8 * time.Second).ShouldNot(HaveOccurred())

		Eventually(func() bool {
			return len(superProducer.getProducers()) == 2
		}).WithTimeout(5 * time.Second).Should(BeTrue())

		time.Sleep(1 * time.Second)
		Eventually(func() bool { mutex.Lock(); defer mutex.Unlock(); return len(reconnectedMap) == 1 },
			300*time.Millisecond).WithTimeout(20 * time.Second).Should(BeTrue())

		Eventually(func() bool {
			return len(superProducer.getProducers()) == 3
		}).WithTimeout(5 * time.Second).Should(BeTrue())

		Expect(superProducer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("should return three key partitions with query route", func() {

		options := NewBindingsOptions([]string{"italy", "spain", "france"})
		Expect(options).NotTo(BeNil())
		Expect(options.getBindingKeys()).To(HaveLen(3))
		Expect(options.getBindingKeys()).To(ConsistOf("italy", "spain", "france"))

		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		var superStream = fmt.Sprintf("key-super-stream-producer-%d", time.Now().Unix())
		Expect(env.DeclareSuperStream(superStream, options.
			SetMaxSegmentSizeBytes(ByteCapacity{}.GB(1)).
			SetMaxAge(3*time.Hour).
			SetBalancedLeaderLocator().
			SetMaxLengthBytes(ByteCapacity{}.KB(1024))),
		).NotTo(HaveOccurred())
		err = env.maybeReconnectLocator()
		Expect(err).NotTo(HaveOccurred())
		Expect(env.locator.client).NotTo(BeNil())
		route, err := env.locator.client.queryRoute(superStream, "italy")
		Expect(err).NotTo(HaveOccurred())
		Expect(route).NotTo(BeNil())
		Expect(route).To(HaveLen(1))
		Expect(route[0]).To(Equal(fmt.Sprintf("%s-italy", superStream)))

		route, err = env.locator.client.queryRoute(superStream, "spain")
		Expect(err).NotTo(HaveOccurred())
		Expect(route).NotTo(BeNil())
		Expect(route).To(HaveLen(1))
		Expect(route[0]).To(Equal(fmt.Sprintf("%s-spain", superStream)))

		route, err = env.locator.client.queryRoute(superStream, "france")
		Expect(err).NotTo(HaveOccurred())
		Expect(route).NotTo(BeNil())
		Expect(route).To(HaveLen(1))
		Expect(route[0]).To(Equal(fmt.Sprintf("%s-france", superStream)))

		// here we test the case where the key is not found
		// the client should return an empty list
		route, err = env.locator.client.queryRoute(superStream, "NOT_EXIST")
		Expect(err).NotTo(HaveOccurred())
		Expect(route).To(Equal([]string{}))

		env.locator.client.Close()
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("should return stream not found query route", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		err = env.maybeReconnectLocator()
		Expect(err).NotTo(HaveOccurred())
		Expect(env.locator.client).NotTo(BeNil())
		route, err := env.locator.client.queryRoute("not-found", "italy")
		Expect(err).To(HaveOccurred())
		Expect(route).To(BeNil())
		env.locator.client.Close()
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("should confirm 6 messages and 1 unRouted", func() {
		// Messages confirmed and unRouted
		// send 3 messages that will be routed to the correct partitions
		// and one message with the wrong key

		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		var superStream = fmt.Sprintf("key-super-stream-producer-with-3-keys-%d", time.Now().Unix())
		countries := []string{"italy", "france", "spain"}
		Expect(env.DeclareSuperStream(superStream,
			NewBindingsOptions(countries))).NotTo(HaveOccurred())

		messagesRouted := make(map[string]int)
		mutex := sync.Mutex{}
		superProducer, err := env.NewSuperStreamProducer(superStream, NewSuperStreamProducerOptions(
			NewKeyRoutingStrategy(
				func(message message.StreamMessage) string {
					return message.GetApplicationProperties()["county"].(string)
				})))

		Expect(err).NotTo(HaveOccurred())

		go func(ch <-chan PartitionPublishConfirm) {
			defer GinkgoRecover()
			for superStreamPublishConfirm := range ch {
				Expect(superStreamPublishConfirm).NotTo(BeNil())
				for _, status := range superStreamPublishConfirm.ConfirmationStatus {
					Expect(status).NotTo(BeNil())
					Expect(status.IsConfirmed()).To(BeTrue())
				}
				mutex.Lock()
				messagesRouted[superStreamPublishConfirm.Partition] += len(superStreamPublishConfirm.ConfirmationStatus)
				mutex.Unlock()
			}
		}(superProducer.NotifyPublishConfirmation(1))

		for _, country := range countries {
			msg := amqp.NewMessage(make([]byte, 0))
			msg.ApplicationProperties = map[string]interface{}{"county": country}
			Expect(superProducer.Send(msg)).NotTo(HaveOccurred())
			// two times the same country in this way we use the cached map
			Expect(superProducer.Send(msg)).NotTo(HaveOccurred())
		}
		msg := amqp.NewMessage(make([]byte, 0))
		msg.ApplicationProperties = map[string]interface{}{"county": "this_country_does_not_exist"}
		err = superProducer.Send(msg)
		Expect(err).To(HaveOccurred())
		Expect(err).To(Equal(ErrMessageRouteNotFound))

		for _, country := range countries {
			Eventually(func() int {
				mutex.Lock()
				defer mutex.Unlock()
				// validate if the messages are confirmed in the correct partition
				return messagesRouted[fmt.Sprintf("%s-%s", superStream, country)]
			}, 300*time.Millisecond).Should(Equal(2))
		}

		Expect(superProducer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("Implement custom routing strategy", func() {

		// TestingRandomStrategy is a custom routing strategy

		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		var superStream = fmt.Sprintf("custom-routing-strategy-%d", time.Now().Unix())
		Expect(env.DeclareSuperStream(superStream, NewPartitionsOptions(3))).NotTo(HaveOccurred())

		superProducer, err := env.NewSuperStreamProducer(superStream, NewSuperStreamProducerOptions(
			NewTestingRandomStrategy(),
		))

		Expect(err).NotTo(HaveOccurred())
		Expect(superProducer).NotTo(BeNil())

		for i := 0; i < 10; i++ {
			msg := amqp.NewMessage(make([]byte, 0))
			Expect(superProducer.Send(msg)).NotTo(HaveOccurred())
		}

		Expect(superProducer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("should return an error when the producer is already connected for a partition", func() {

		// Test is to validate the error when the producer is already connected
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		var superStream = fmt.Sprintf("already-connected-super-stream-producer-%d", time.Now().Unix())
		Expect(env.DeclareSuperStream(superStream, NewPartitionsOptions(3))).NotTo(HaveOccurred())

		superProducer, err := env.NewSuperStreamProducer(superStream, NewSuperStreamProducerOptions(
			NewTestingRandomStrategy(),
		))

		Expect(err).NotTo(HaveOccurred())

		err = superProducer.ConnectPartition("this-partition-does-not-exist")
		Expect(err).To(HaveOccurred())

		err = superProducer.ConnectPartition("already-connected-super-stream-producer-0")
		Expect(err).To(HaveOccurred())

		Expect(superProducer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("should reconnect to the same partition after a close event", func() {
		const partitionsCount = 3
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())

		var superStream = fmt.Sprintf("reconnect-test-super-stream-%d", time.Now().Unix())
		Expect(env.DeclareSuperStream(superStream, NewPartitionsOptions(partitionsCount))).NotTo(HaveOccurred())

		superProducer, err := newSuperStreamProducer(env, superStream, &SuperStreamProducerOptions{
			RoutingStrategy: NewHashRoutingStrategy(func(msg message.StreamMessage) string {
				return msg.GetApplicationProperties()["routingKey"].(string)
			}),
		})
		Expect(err).To(BeNil())
		Expect(superProducer).NotTo(BeNil())
		Expect(superProducer.init()).NotTo(HaveOccurred())
		producers := superProducer.getProducers()
		Expect(producers).To(HaveLen(partitionsCount))
		partitionToClose := producers[0].GetStreamName()

		// Declare synchronization helpers and listeners
		partitionCloseEvent := make(chan bool)

		// Listen for the partition close event and try to reconnect
		go func(ch <-chan PPartitionClose) {
			for event := range ch {
				err := event.Context.ConnectPartition(event.Partition)
				Expect(err).To(BeNil())

				partitionCloseEvent <- true

				break

			}
		}(superProducer.NotifyPartitionClose(1))

		// Imitates metadataUpdateFrameHandler - it can happen when stream members are changed.
		go func() {
			client, ok := env.producers.getCoordinators()["localhost:5552"].clientsPerContext.Load(1)
			Expect(ok).To(BeTrue())
			client.(*Client).maybeCleanProducers(partitionToClose)
		}()

		// Wait for the partition close event
		Eventually(partitionCloseEvent).WithTimeout(5 * time.Second).WithPolling(100 * time.Millisecond).Should(Receive())

		// Verify that the partition was successfully reconnected
		Eventually(superProducer.getProducers()).WithTimeout(5 * time.Second).WithPolling(100 * time.Millisecond).Should(HaveLen(partitionsCount))
		reconnectedProducer := superProducer.getProducer(partitionToClose)
		Expect(reconnectedProducer).NotTo(BeNil())

		// Clean up
		Expect(superProducer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})
	It("should detect potential data races when sending concurrently", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		var superStream = fmt.Sprintf("race-super-stream-%d", time.Now().Unix())
		Expect(env.DeclareSuperStream(superStream, NewPartitionsOptions(10))).NotTo(HaveOccurred())

		superProducer, err := env.NewSuperStreamProducer(superStream, NewSuperStreamProducerOptions(
			NewHashRoutingStrategy(func(message message.StreamMessage) string {
				return message.GetApplicationProperties()["routingKey"].(string)
			}),
		))
		Expect(err).NotTo(HaveOccurred())

		// example error handling from producer on the client side
		go func() {
			superProducer.NotifyPartitionClose(1)
		}()

		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})
})
