package stream

import (
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strconv"
	"sync"
	"time"
)

var client *Client
var streamName string
var _ = BeforeSuite(func() {
	client = NewStreamingClient()
	streamName = uuid.New().String()
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
			code, err := client.CreateStream(streamName)
			Expect(err).NotTo(HaveOccurred())
			Expect(client.responses.Count()).To(Equal(0))
			Expect(code.id).To(Equal(ResponseCodeOk))

		})
		It("New/Close Publisher", func() {
			producer, err := client.NewProducer(streamName)
			Expect(err).NotTo(HaveOccurred())
			err = producer.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("New/Publish/Close Publisher", func() {
			producer, err := client.NewProducer(streamName)
			Expect(err).NotTo(HaveOccurred())
			var arr []*amqp.Message
			for z := 0; z < 10; z++ {
				arr = append(arr, amqp.NewMessage([]byte("test_"+strconv.Itoa(z))))
			}
			_, err = producer.BatchPublish(nil, arr) // batch send
			Expect(err).NotTo(HaveOccurred())
			// we can't close the subscribe until the publish is finished
			time.Sleep(500 * time.Millisecond)
			err = producer.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("Multi-thread New/Publish/Close Publisher", func() {
			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					producer, err := client.NewProducer(streamName)
					Expect(err).NotTo(HaveOccurred())
					var arr []*amqp.Message
					for z := 0; z < 5; z++ {
						arr = append(arr, amqp.NewMessage([]byte("test_"+strconv.Itoa(z))))
					}
					_, err = producer.BatchPublish(nil, arr) // batch send
					Expect(err).NotTo(HaveOccurred())
					// we can't close the subscribe until the publish is finished
					time.Sleep(500 * time.Millisecond)
					err = producer.Close()
					Expect(err).NotTo(HaveOccurred())
				}(&wg)
			}
			wg.Wait()
		})
		It("Delete Stream", func() {
			code, err := client.DeleteStream(streamName)
			Expect(err).NotTo(HaveOccurred())
			Expect(code.id).To(Equal(ResponseCodeOk))
		})
		It("Create two times Stream", func() {
			code, err := client.CreateStream(streamName)
			Expect(err).NotTo(HaveOccurred())
			Expect(code.id).To(Equal(ResponseCodeOk))

			code, err = client.CreateStream(streamName)
			Expect(err).NotTo(HaveOccurred())
			Expect(code.id).To(Equal(ResponseCodeStreamAlreadyExists))

			code, err = client.DeleteStream(streamName)
			Expect(err).NotTo(HaveOccurred())
			Expect(code.id).To(Equal(ResponseCodeOk))
		})


	})
})
