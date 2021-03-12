package stream

import (
	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strconv"
	"time"
)

var client *Client
var _ = BeforeSuite(func() {
	client = NewStreamingClient()
})

var _ = AfterSuite(func() {
	Expect(client.producers.Count()).To(Equal(0))
	Expect(client.responses.Count()).To(Equal(0))
})

var _ = Describe("Streaming client", func() {
	BeforeEach(func() {

	})
	AfterEach(func() {
	})

	Describe("Streaming client", func() {
		It("Connection ", func() {
			err := client.Connect("rabbitmq-stream://guest:guest@localhost:5551/%2f")
			Expect(err).NotTo(HaveOccurred())
		})
		It("Create Stream", func() {
			err := client.CreateStream("test-stream")
			Expect(err).NotTo(HaveOccurred())
		})
		It("New/Close Publisher", func() {
			producer, err := client.NewProducer("test-stream")
			Expect(err).NotTo(HaveOccurred())
			err = producer.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("New/Publish/Close Publisher", func() {
			producer, err := client.NewProducer("test-stream")
			Expect(err).NotTo(HaveOccurred())
			var arr []*amqp.Message
			for z := 0; z < 10; z++ {
				arr = append(arr, amqp.NewMessage([]byte("test_"+strconv.Itoa(z))))
			}
			_, err = producer.BatchPublish(nil, arr) // batch send
			Expect(err).NotTo(HaveOccurred())
			// we can't close the producer until the publish is finished
			time.Sleep(1 * time.Second)
			err = producer.Close()
			Expect(err).NotTo(HaveOccurred())
		})



	})
})
