package raw_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
)

var _ = Describe("ClientTypes", func() {
	Context("Raw Client Configurations", func() {
		It("creates a new client configuration", func() {
			clientConf, err := raw.NewClientConfiguration(
				"rabbitmq-stream://foo:bar@localparty.com:4321/party-vhost")
			Expect(err).ToNot(HaveOccurred())

			brokers := clientConf.RabbitmqBroker()
			Expect(brokers).To(MatchFields(IgnoreExtras,
				Fields{
					"Host":     Equal("localparty.com"),
					"Port":     BeNumerically("==", 4321),
					"Username": Equal("foo"),
					"Password": Equal("bar"),
					"Vhost":    Equal("party-vhost"),
					"Scheme":   Equal("rabbitmq-stream"),
				}))
		})

		It("accepts zero URLs and returns default Broker", func() {
			conf, err := raw.NewClientConfiguration("")
			Expect(err).ToNot(HaveOccurred())

			broker := conf.RabbitmqBroker()
			Expect(broker).NotTo(BeNil())
			Expect(broker).To(MatchFields(IgnoreExtras,
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
					b, err := raw.NewClientConfiguration(uri)
					Expect(err).To(HaveOccurred())
					Expect(b).To(BeNil())
				}
			})
		})
	})
})
