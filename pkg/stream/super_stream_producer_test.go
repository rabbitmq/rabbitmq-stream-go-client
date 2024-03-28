package stream

import (
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

})
