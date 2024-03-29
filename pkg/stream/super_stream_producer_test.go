package stream

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
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

	It("should create a new super stream producer", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		const superStream = "first-super-stream-producer"
		Expect(env.DeclareSuperStream(superStream, NewPartitionsSuperStreamOptions(3))).NotTo(HaveOccurred())
		superProducer := newSuperStreamProducer(env, superStream, &SuperStreamProducerOptions{})
		Expect(superProducer).NotTo(BeNil())
		Expect(superProducer.init()).NotTo(HaveOccurred())
		Expect(superProducer.producers).To(HaveLen(3))
		Expect(superProducer.Close()).NotTo(HaveOccurred())
		Expect(superProducer.producers).To(HaveLen(0))
		Expect(env.DeleteSuperStream(superStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("should Send messages and confirmed to all the streams", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		// we do this test to be sure that the producer is able to Send messages to all the partitions
		// the same was done in .NET client and python client
		const superStream = "invoices"
		Expect(env.DeclareSuperStream(superStream, NewPartitionsSuperStreamOptions(3))).NotTo(HaveOccurred())
		superProducer := newSuperStreamProducer(env, superStream, &SuperStreamProducerOptions{
			RoutingStrategy: NewHashRoutingMurmurStrategy(func(message message.StreamMessage) string {
				return message.GetApplicationProperties()["routingKey"].(string)
			}),
		})

		Expect(superProducer).NotTo(BeNil())
		Expect(superProducer.init()).NotTo(HaveOccurred())
		Expect(superProducer.producers).To(HaveLen(3))

		for i := 0; i < 20; i++ {

			msg := amqp.NewMessage(make([]byte, 0))
			msg.ApplicationProperties = map[string]interface{}{"routingKey": fmt.Sprintf("hello%d", i)}
			Expect(superProducer.Send(msg)).NotTo(HaveOccurred())
		}

		Expect(superProducer.Close()).NotTo(HaveOccurred())
		//env.DeleteSuperStream(superStream)

	})
})
