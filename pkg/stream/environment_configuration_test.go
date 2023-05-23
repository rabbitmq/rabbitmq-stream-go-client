package stream_test

import (
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/stream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("EnvironmentConfiguration", Focus, func() {
	It("creates a configuration with expected defaults", func() {
		ec := stream.NewEnvironmentConfiguration()

		const expectedUri = "rabbitmq-stream://guest:guest@localhost:5552/%2f"
		Expect(ec.Uri).To(Equal(expectedUri))
		Expect(ec.Uris).To(ConsistOf(expectedUri))

		Expect(ec.MaxProducersByConnection).To(Equal(255))
		Expect(ec.MaxConsumersByConnection).To(Equal(255))
		Expect(ec.MaxTrackingConsumersByConnection).To(Equal(50))

		Expect(ec.Host).To(Equal("localhost"))
		Expect(ec.Port).To(Equal(5552))
		Expect(ec.Username).To(Equal("guest"))
		Expect(ec.Password).To(Equal("guest"))

		Expect(ec.VirtualHost).To(Equal("/"))
		Expect(ec.Id).To(Equal("rabbitmq-stream"))
		Expect(ec.LazyInitialization).To(BeFalse(), "expected lazy initialization to have default False")
	})

	When("is configured with an URI option", func() {
		It("configures all attributes correctly", func() {
			ec := stream.NewEnvironmentConfiguration(stream.WithUri("rabbitmq-stream://foo:bar@192.1.1.10:1234/my-vhost"))

			const expectedUri = "rabbitmq-stream://foo:bar@192.1.1.10:1234/my-vhost"
			Expect(ec.Uri).To(Equal(expectedUri))
			Expect(ec.Uris).To(ConsistOf(expectedUri))

			Expect(ec.Username).To(Equal("foo"))
			Expect(ec.Password).To(Equal("bar"))
			Expect(ec.Host).To(Equal("192.1.1.10"))
			Expect(ec.Port).To(Equal(1234))
			Expect(ec.VirtualHost).To(Equal("my-vhost"))
		})
	})

	When("it's configured with multiple URIs option", func() {
		It("configures the attributes with the first URI", func() {
			ec := stream.NewEnvironmentConfiguration(
				stream.WithUris(
					"rabbitmq-stream://user:pass@somehost:12345/some-vhost",
					"rabbitmq-stream://homer:simpson@springfield.city:4567/nuclear_plant",
				))

			const (
				expectedFirstUri  = "rabbitmq-stream://user:pass@somehost:12345/some-vhost"
				expectedSecondUri = "rabbitmq-stream://homer:simpson@springfield.city:4567/nuclear_plant"
			)
			Expect(ec.Uri).To(Equal(expectedFirstUri))
			Expect(ec.Uris).To(ConsistOf(expectedFirstUri, expectedSecondUri))

			Expect(ec.Username).To(Equal("user"))
			Expect(ec.Password).To(Equal("pass"))
			Expect(ec.Host).To(Equal("somehost"))
			Expect(ec.Port).To(Equal(12345))
			Expect(ec.VirtualHost).To(Equal("some-vhost"))
		})
	})

	When("it's configured with max producers by connection option", func() {
		It("configures the max producers attributes", func() {
			ec := stream.NewEnvironmentConfiguration(
				stream.WithMaxProducersByConnection(42),
			)

			Expect(ec.MaxProducersByConnection).To(Equal(42))
		})

		When("a negative value is provided", func() {
			It("configures the minimum allowed value", func() {
				ec := stream.NewEnvironmentConfiguration(
					stream.WithMaxProducersByConnection(-123),
				)

				Expect(ec.MaxProducersByConnection).To(Equal(1))
			})
		})

		When("a higher than allowed value is provided", func() {
			It("configures the maximum allowed value", func() {
				ec := stream.NewEnvironmentConfiguration(
					stream.WithMaxProducersByConnection(10_000),
				)

				Expect(ec.MaxProducersByConnection).To(Equal(255))
			})
		})
	})

	When("it's configured with max tracking consumers by connection option", func() {
		It("configures the max tracking consumers attributes", func() {
			ec := stream.NewEnvironmentConfiguration(
				stream.WithMaxTrackingConsumersByConnection(111),
			)

			Expect(ec.MaxTrackingConsumersByConnection).To(Equal(111))
		})

		When("a negative value is provided", func() {
			It("configures the minimum allowed value", func() {
				ec := stream.NewEnvironmentConfiguration(
					stream.WithMaxTrackingConsumersByConnection(-123),
				)

				Expect(ec.MaxTrackingConsumersByConnection).To(Equal(1))
			})
		})

		When("a higher than allowed value is provided", func() {
			It("configures the maximum allowed value", func() {
				ec := stream.NewEnvironmentConfiguration(
					stream.WithMaxTrackingConsumersByConnection(10_000),
				)

				Expect(ec.MaxTrackingConsumersByConnection).To(Equal(255))
			})
		})
	})

	When("it's configured with max consumers by connection option", func() {
		It("configures the max consumer attributes", func() {
			ec := stream.NewEnvironmentConfiguration(
				stream.WithMaxConsumersByConnection(42),
			)

			Expect(ec.MaxConsumersByConnection).To(Equal(42))
		})

		When("a negative value is provided", func() {
			It("configures the minimum allowed value", func() {
				ec := stream.NewEnvironmentConfiguration(
					stream.WithMaxConsumersByConnection(-123),
				)

				Expect(ec.MaxConsumersByConnection).To(Equal(1))
			})
		})

		When("a higher than allowed value is provided", func() {
			It("configures the maximum allowed value", func() {
				ec := stream.NewEnvironmentConfiguration(
					stream.WithMaxConsumersByConnection(10_000),
				)

				Expect(ec.MaxConsumersByConnection).To(Equal(255))
			})
		})
	})
})
