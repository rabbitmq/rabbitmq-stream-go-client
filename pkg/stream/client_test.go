package stream

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var client1 *Client
var _ = BeforeSuite(func() {
	client1 = NewStreamingClient()
})

var _ = Describe("Streaming client", func() {
	BeforeEach(func() {

	})
	AfterEach(func() {
	})

	Describe("Streaming client", func() {
		It("Connection ", func() {
			err := client1.Connect("rabbitmq-stream://guest:guest@localhost:5551/%2f")
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
