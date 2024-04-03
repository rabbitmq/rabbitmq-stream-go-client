package stream

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/test_helper"
	"sync"
	"time"
)

var _ = Describe("Super Stream Producer", Label("super-stream"), func() {

	DescribeTable("Partitioning using Murmur3",
		func(key string, partition string) {

			routingMurmur := NewHashRoutingMurmurStrategy(func(message message.StreamMessage) string {
				return message.GetApplicationProperties()["routingKey"].(string)
			})
			Expect(routingMurmur).NotTo(BeNil())

			partitions := []string{"invoices-01", "invoices-02", "invoices-03"}

			msg := amqp.NewMessage(make([]byte, 0))
			msg.ApplicationProperties = map[string]interface{}{"routingKey": key}
			msg.MarshalBinary()
			routing := routingMurmur.Route(msg, partitions)
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
		producer, err := newSuperStreamProducer(nil, "it_does_not_matter", nil)
		Expect(producer).To(BeNil())
		Expect(err).To(Equal(ErrEnvironmentNotDefined))

		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())

		producer, err = newSuperStreamProducer(env, "", nil)
		Expect(producer).To(BeNil())
		Expect(err).To(HaveOccurred())

		producer, err = newSuperStreamProducer(env, "    ", nil)
		Expect(producer).To(BeNil())
		Expect(err).To(HaveOccurred())

		producer, err = newSuperStreamProducer(env, "it_does_not_matter", nil)
		Expect(producer).To(BeNil())
		Expect(err).To(Equal(ErrSuperStreamProducerOptionsNotDefined))

		producer, err = newSuperStreamProducer(env, "it_does_not_matter", &SuperStreamProducerOptions{})
		Expect(producer).To(BeNil())
		Expect(err).To(Equal(ErrSuperStreamProducerOptionsNotDefined))

		producer, err = newSuperStreamProducer(env, "it_does_not_matter", &SuperStreamProducerOptions{
			RoutingStrategy: NewHashRoutingMurmurStrategy(func(message message.StreamMessage) string {
				return message.GetApplicationProperties()["routingKey"].(string)
			}),
		})
		Expect(producer).To(BeNil())
		Expect(err).To(Equal(ErrSuperStreamProducerOptionsNotDefined))

	})

	It("should create a new super stream producer", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		const superStream = "first-super-stream-producer"
		Expect(env.DeclareSuperStream(superStream, NewPartitionsSuperStreamOptions(3))).NotTo(HaveOccurred())
		superProducer, err := newSuperStreamProducer(env, superStream, &SuperStreamProducerOptions{
			RoutingStrategy: NewHashRoutingMurmurStrategy(func(message message.StreamMessage) string {

				return message.GetApplicationProperties()["routingKey"].(string)
			}),
			HandleSuperStreamConfirmation: func(partition string, confirmationStatus *SuperStreamPublishConfirm) {

			},
		})
		Expect(err).To(BeNil())
		Expect(superProducer).NotTo(BeNil())
		Expect(superProducer.init()).NotTo(HaveOccurred())
		Expect(superProducer.producers).To(HaveLen(3))
		Expect(superProducer.Close()).NotTo(HaveOccurred())
		//Expect(superProducer.producers).To(HaveLen(0))
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("should Send messages and confirmed to all the streams", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		// we do this test to be sure that the producer is able to Send messages to all the partitions
		// the same was done in .NET client and python client
		const superStream = "invoices"

		msgReceived := make(map[string]int)
		mutex := sync.Mutex{}
		Expect(env.DeclareSuperStream(superStream, NewPartitionsSuperStreamOptions(3))).NotTo(HaveOccurred())
		superProducer, err := newSuperStreamProducer(env, superStream,
			&SuperStreamProducerOptions{
				RoutingStrategy: NewHashRoutingMurmurStrategy(func(message message.StreamMessage) string {
					return message.GetApplicationProperties()["routingKey"].(string)
				}),
				HandleSuperStreamConfirmation: func(partition string, confirmationStatus *SuperStreamPublishConfirm) {
					Expect(confirmationStatus).NotTo(BeNil())
					for _, status := range confirmationStatus.ConfirmationStatus {
						Expect(status).NotTo(BeNil())
						Expect(status.IsConfirmed()).To(BeTrue())
					}
					mutex.Lock()
					msgReceived[partition] = len(confirmationStatus.ConfirmationStatus)
					mutex.Unlock()
				},
			})

		Expect(err).To(BeNil())
		Expect(superProducer).NotTo(BeNil())
		Expect(superProducer.init()).NotTo(HaveOccurred())
		Expect(superProducer.producers).To(HaveLen(3))

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
		Expect(env.DeclareSuperStream(superStream, NewPartitionsSuperStreamOptions(3))).NotTo(HaveOccurred())
		superProducer, err := newSuperStreamProducer(env, superStream, &SuperStreamProducerOptions{
			RoutingStrategy: NewHashRoutingMurmurStrategy(func(message message.StreamMessage) string {
				return message.GetApplicationProperties()["routingKey"].(string)
			}),
			HandleSuperStreamConfirmation: func(partition string, confirmationStatus *SuperStreamPublishConfirm) {

			},
			HandlePartitionClose: func(partition string, event Event, context PartitionContext) {
				mutex.Lock()
				defer mutex.Unlock()
				Expect(event.Reason).To(Equal("deletePublisher"))
				closedMap[partition] = true
			},
		})

		Expect(superProducer).NotTo(BeNil())
		Expect(err).To(BeNil())
		Expect(superProducer.init()).NotTo(HaveOccurred())
		Expect(superProducer.producers).To(HaveLen(3))
		Expect(superProducer.Close()).NotTo(HaveOccurred())

		Eventually(func() bool { mutex.Lock(); defer mutex.Unlock(); return len(closedMap) == 3 },
			300*time.Millisecond).WithTimeout(5 * time.Second).Should(BeTrue())

		Expect(superProducer.producers).To(HaveLen(0))
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("should handle reconnect the producer for the partition ", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		const superStream = "reconnect-super-stream-producer"

		Expect(env.DeclareSuperStream(superStream, NewPartitionsSuperStreamOptions(3))).NotTo(HaveOccurred())

		var reconnectedMap = make(map[string]bool)
		mutex := sync.Mutex{}
		superProducer, err := newSuperStreamProducer(env, superStream, &SuperStreamProducerOptions{
			RoutingStrategy: NewHashRoutingMurmurStrategy(func(message message.StreamMessage) string {
				return message.GetApplicationProperties()["routingKey"].(string)
			}),
			HandleSuperStreamConfirmation: func(partition string, confirmationStatus *SuperStreamPublishConfirm) {

			},
			HandlePartitionClose: func(partition string, event Event, context PartitionContext) {
				defer GinkgoRecover()
				mutex.Lock()
				defer mutex.Unlock()
				if event.Reason == SocketClosed {
					time.Sleep(2 * time.Second)
					Expect(context.ConnectPartition(partition)).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
					reconnectedMap[partition] = true
				}
			},
			ClientProvidedName: "reconnect-super-stream-producer",
		})
		Expect(superProducer).NotTo(BeNil())
		Expect(err).To(BeNil())
		Expect(superProducer.init()).NotTo(HaveOccurred())

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

		options := NewBindingsSuperStreamOptions([]string{"italy", "spain", "france"})
		Expect(options).NotTo(BeNil())
		Expect(options.getBindingKeys()).To(HaveLen(3))
		Expect(options.getBindingKeys()).To(ConsistOf("italy", "spain", "france"))

		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		const superStream = "key-super-stream-producer"
		Expect(env.DeclareSuperStream(superStream, options)).NotTo(HaveOccurred())
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
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		const superStream = "key-super-stream-producer-with-3-keys"
		countries := []string{"italy", "france", "spain"}
		Expect(env.DeclareSuperStream(superStream,
			NewBindingsSuperStreamOptions(countries))).NotTo(HaveOccurred())

		messagesRouted := make(map[string]int)
		messagesUnRouted := 0
		mutex := sync.Mutex{}
		superProducer, err := env.NewSuperStreamProducer(superStream, &SuperStreamProducerOptions{
			RoutingStrategy: NewKeyRoutingStrategy(
				func(message message.StreamMessage) string {
					return message.GetApplicationProperties()["county"].(string)
				}, func(message message.StreamMessage, cause error) {
					defer GinkgoRecover()
					Expect(cause).To(HaveOccurred())
					mutex.Lock()
					messagesUnRouted++
					mutex.Unlock()
				}),
			HandleSuperStreamConfirmation: func(partition string, confirmationStatus *SuperStreamPublishConfirm) {
				mutex.Lock()
				defer mutex.Unlock()
				messagesRouted[partition] += len(confirmationStatus.ConfirmationStatus)
			},
		})

		for _, country := range countries {
			msg := amqp.NewMessage(make([]byte, 0))
			msg.ApplicationProperties = map[string]interface{}{"county": country}
			Expect(superProducer.Send(msg)).NotTo(HaveOccurred())
			// two times the same country in this way we use the cached map
			Expect(superProducer.Send(msg)).NotTo(HaveOccurred())
		}

		msg := amqp.NewMessage(make([]byte, 0))
		msg.ApplicationProperties = map[string]interface{}{"county": "this_country_does_not_exist"}
		Expect(superProducer.Send(msg)).NotTo(HaveOccurred())

		time.Sleep(1 * time.Second)
		Eventually(func() bool { mutex.Lock(); defer mutex.Unlock(); return messagesUnRouted == 1 }, 300*time.Millisecond).
			WithTimeout(5 * time.Second).Should(BeTrue())

		for _, country := range countries {
			Eventually(func() int {
				mutex.Lock()
				defer mutex.Unlock()
				return messagesRouted[fmt.Sprintf("%s-%s", superStream, country)]
			}, 300*time.Millisecond).Should(Equal(2))
		}

		Expect(superProducer.Close()).NotTo(HaveOccurred())
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
		Expect(err).NotTo(HaveOccurred())
	})

})
