package stream

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/test-helper"
	"math/rand"
	"sync"
	"time"
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

var _ = Describe("Super Stream Producer", Label("super-stream"), func() {

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
			msg.MarshalBinary()
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
		const superStream = "first-super-stream-producer"
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
		const superStream = "invoices"

		msgReceived := make(map[string]int)
		mutex := sync.Mutex{}
		Expect(env.DeclareSuperStream(superStream, NewPartitionsOptions(3))).NotTo(HaveOccurred())
		superProducer, err := newSuperStreamProducer(env, superStream,
			&SuperStreamProducerOptions{
				RoutingStrategy: NewHashRoutingStrategy(func(message message.StreamMessage) string {
					return message.GetApplicationProperties()["routingKey"].(string)
				})})

		go func(ch <-chan PartitionPublishConfirm) {
			defer GinkgoRecover()
			for superStreamPublishConfirm := range ch {
				Expect(superStreamPublishConfirm).NotTo(BeNil())
				for _, status := range superStreamPublishConfirm.ConfirmationStatus {
					Expect(status).NotTo(BeNil())
					Expect(status.IsConfirmed()).To(BeTrue())
				}
				mutex.Lock()
				msgReceived[superStreamPublishConfirm.Partition] = len(superStreamPublishConfirm.ConfirmationStatus)
				mutex.Unlock()
			}

		}(superProducer.NotifyPublishConfirmation())

		Expect(err).To(BeNil())
		Expect(superProducer).NotTo(BeNil())
		Expect(superProducer.init()).NotTo(HaveOccurred())
		Expect(superProducer.activeProducers).To(HaveLen(3))

		for i := 0; i < 20; i++ {
			msg := amqp.NewMessage(make([]byte, 0))
			msg.ApplicationProperties = map[string]interface{}{"routingKey": fmt.Sprintf("hello%d", i)}
			Expect(superProducer.Send(msg)).NotTo(HaveOccurred())
		}

		time.Sleep(1 * time.Second)
		// these values are the same for .NET,Python,Java stream clients
		// The aim for this test is to validate the correct routing with the
		// MurmurStrategy.
		Eventually(func() bool {
			mutex.Lock()
			defer mutex.Unlock()
			return msgReceived["invoices-0"] == 9
		}, 300*time.Millisecond).WithTimeout(8 * time.Second).Should(BeTrue())
		Eventually(func() bool {
			mutex.Lock()
			defer mutex.Unlock()
			return msgReceived["invoices-1"] == 7
		}, 300*time.Millisecond).WithTimeout(8 * time.Second).Should(BeTrue())
		Eventually(func() bool {
			mutex.Lock()
			defer mutex.Unlock()
			return msgReceived["invoices-2"] == 4
		}, 300*time.Millisecond).WithTimeout(8 * time.Second).Should(BeTrue())

		Expect(superProducer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("should handle three close ( one for partition )", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		const superStream = "close-super-stream-producer"
		var closedMap = make(map[string]bool)
		mutex := sync.Mutex{}
		Expect(env.DeclareSuperStream(superStream,
			NewPartitionsOptions(3).
				SetClientLocalLocator())).NotTo(HaveOccurred())
		superProducer, err := newSuperStreamProducer(env, superStream, NewSuperStreamProducerOptions(
			NewHashRoutingStrategy(func(message message.StreamMessage) string {
				return message.GetApplicationProperties()["routingKey"].(string)
			})))

		go func(ch <-chan PartitionClose) {
			defer GinkgoRecover()
			for chq := range ch {
				mutex.Lock()
				Expect(chq.Event.Reason).To(Equal("deletePublisher"))
				closedMap[chq.Partition] = true
				mutex.Unlock()
			}

		}(superProducer.NotifyPartitionClose())

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
		// with context PartitionContext

		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		const superStream = "reconnect-super-stream-producer"

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

		go func(ch <-chan PartitionClose) {
			defer GinkgoRecover()
			for chq := range ch {
				if chq.Event.Reason == SocketClosed {
					time.Sleep(2 * time.Second)
					Expect(chq.Context.ConnectPartition(chq.Partition)).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
					mutex.Lock()
					reconnectedMap[chq.Partition] = true
					mutex.Unlock()

				}
			}

		}(superProducer.NotifyPartitionClose())

		time.Sleep(3 * time.Second)
		Eventually(func() error {
			return test_helper.DropConnectionClientProvidedName("reconnect-super-stream-producer", "15672")
		}, 300*time.Millisecond).WithTimeout(8 * time.Second).ShouldNot(HaveOccurred())

		Eventually(func() bool {
			return len(superProducer.getProducers()) == 2
		}).WithTimeout(5 * time.Second).Should(BeTrue())

		time.Sleep(1 * time.Second)
		Eventually(func() bool { mutex.Lock(); defer mutex.Unlock(); return len(reconnectedMap) == 1 },
			300*time.Millisecond).WithTimeout(5 * time.Second).Should(BeTrue())

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
		const superStream = "key-super-stream-producer"
		Expect(env.DeclareSuperStream(superStream, options.
			SetMaxSegmentSizeBytes(ByteCapacity{}.GB(1)).
			SetMaxAge(3*time.Hour).
			SetBalancedLeaderLocator().
			SetMaxLengthBytes(ByteCapacity{}.KB(1024))),
		).NotTo(HaveOccurred())
		client, err := env.newReconnectClient()
		Expect(err).NotTo(HaveOccurred())
		Expect(client).NotTo(BeNil())
		route, err := client.queryRoute(superStream, "italy")
		Expect(err).NotTo(HaveOccurred())
		Expect(route).NotTo(BeNil())
		Expect(route).To(HaveLen(1))
		Expect(route[0]).To(Equal("key-super-stream-producer-italy"))

		route, err = client.queryRoute(superStream, "spain")
		Expect(err).NotTo(HaveOccurred())
		Expect(route).NotTo(BeNil())
		Expect(route).To(HaveLen(1))
		Expect(route[0]).To(Equal("key-super-stream-producer-spain"))

		route, err = client.queryRoute(superStream, "france")
		Expect(err).NotTo(HaveOccurred())
		Expect(route).NotTo(BeNil())
		Expect(route).To(HaveLen(1))
		Expect(route[0]).To(Equal("key-super-stream-producer-france"))

		// here we test the case where the key is not found
		// the client should return an empty list
		route, err = client.queryRoute(superStream, "NOT_EXIST")
		Expect(err).NotTo(HaveOccurred())
		Expect(route).To(Equal([]string{}))

		Expect(client.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("should return stream not found query route", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		client, err := env.newReconnectClient()
		Expect(err).NotTo(HaveOccurred())
		Expect(client).NotTo(BeNil())
		route, err := client.queryRoute("not-found", "italy")
		Expect(err).To(HaveOccurred())
		Expect(route).To(BeNil())
		Expect(client.Close()).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("should confirm 6 messages and 1 unRouted", func() {
		// Messages confirmed and unRouted
		// send 3 messages that will be routed to the correct partitions
		// and one message with the wrong key

		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		const superStream = "key-super-stream-producer-with-3-keys"
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
		}(superProducer.NotifyPublishConfirmation())

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
		const superStream = "custom-routing-strategy"
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
		const superStream = "already-connected-super-stream-producer"
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

})
