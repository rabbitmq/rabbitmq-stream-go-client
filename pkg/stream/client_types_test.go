package stream_test

import (
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/stream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("ClientTypes", func() {
	Context("Raw Client Configurations", func() {
		It("creates a new client configuration", func() {
			clientConf, err := stream.NewRawClientConfiguration(
				"rabbitmq-stream://foo:bar@localparty.com:4321/party-vhost")
			Expect(err).ToNot(HaveOccurred())

			brokers := clientConf.RabbitmqBrokers()
			Expect(brokers).NotTo(BeNil())
			Expect(len(brokers)).To(BeNumerically("==", 1))
			Expect(brokers[0]).To(MatchFields(IgnoreExtras,
				Fields{
					"Host":     Equal("localparty.com"),
					"Port":     BeNumerically("==", 4321),
					"Username": Equal("foo"),
					"Password": Equal("bar"),
					"Vhost":    Equal("party-vhost"),
					"Scheme":   Equal("rabbitmq-stream"),
				}))
		})

		It("accepts multiple uris",  func() {
			conf, err := stream.NewRawClientConfiguration(
				"rabbitmq-stream://localhost:5552",
				"rabbitmq-stream://example.com",
				"rabbitmq-stream+tls://user:pass@some.service.com:4242/a-vhost")
			Expect(err).ToNot(HaveOccurred())

			brokers := conf.RabbitmqBrokers()
			Expect(brokers).NotTo(BeNil())
			Expect(brokers).To(HaveLen(3))
			Expect(brokers[0]).To(MatchFields(IgnoreExtras,
				Fields{
					"Host":     Equal("localhost"),
					"Port":     BeNumerically("==", 5552),
					"Username": Equal("guest"),
					"Password": Equal("guest"),
					"Vhost":    Equal("/"),
					"Scheme":   Equal("rabbitmq-stream"),
				}))

			Expect(brokers[1]).To(MatchFields(IgnoreExtras,
				Fields{
					"Host":     Equal("example.com"),
					"Port":     BeNumerically("==", 5552),
					"Username": Equal("guest"),
					"Password": Equal("guest"),
					"Vhost":    Equal("/"),
					"Scheme":   Equal("rabbitmq-stream"),
				}))

			Expect(brokers[2]).To(MatchFields(IgnoreExtras,
				Fields{
					"Host":     Equal("some.service.com"),
					"Port":     BeNumerically("==", 4242),
					"Username": Equal("user"),
					"Password": Equal("pass"),
					"Vhost":    Equal("a-vhost"),
					"Scheme":   Equal("rabbitmq-stream+tls"),
				}))
		})

		It("accepts zero URLs and returns default broker", func() {
			conf, err := stream.NewRawClientConfiguration()
			Expect(err).ToNot(HaveOccurred())

			broker := conf.RabbitmqBrokers()
			Expect(broker).NotTo(BeNil())
			Expect(broker).To(HaveLen(1))
			Expect(broker[0]).To(MatchFields(IgnoreExtras,
				Fields{
					"Host":     Equal("localhost"),
					"Port":     BeNumerically("==", 5552),
					"Username": Equal("guest"),
					"Password": Equal("guest"),
					"Vhost":    Equal("/"),
					"Scheme":   Equal("rabbitmq-stream"),
				}))
		})

		When("RabbitMQ URL is invalid", func() {
			It("returns an error", func() {
				uris := []string{
					"foobar://localhost:5552/vhost",
					"rabbitmq-stream://user name:password@local/vhost",
					"thisIsNotAnUrl",
				}

				for _, uri := range uris {
					b, err := stream.NewRawClientConfiguration(uri)
					Expect(err).To(HaveOccurred())
					Expect(b).To(BeNil())
				}
			})
		})
	})
})
